--!nocheck

local DataService = { Client = {} }
DataService.Client.Server = DataService

---------------------
-- Roblox Services --
---------------------
local DatastoreService = game:GetService("DataStoreService")
local Players = game:GetService("Players")
local HttpService = game:GetService("HttpService")

------------------
-- Dependencies --
------------------
local RobloxLibModules = require(script.Parent["roblox-libmodules"])
local Table = require(script.Parent.Table)
local Queue = require(RobloxLibModules.Classes.Queue)

-------------
-- Defines --
-------------
local OPERATION_MAX_RETRIES = 3 -- The number of times any data operation will be attempted before aborting
local OPERATION_RETRY_INTERVAL = 5 -- The number of seconds between each retry for any data operation. Recommended to keep this above 4 seconds, as Roblox GetAsync() calls cache data for 4 seconds.
local DATASTORE_NAME = ""
local DATA_SCHEMA = {
	Version = 1,
	Data = {},
	Migrators = {},
}
local DATA_HANDLER_MODULE = nil
local DATA_WRITERS = {}
local DATA_READERS = {}
local EVENTS = {}
local WasConfigured = false
local DataCaches = {}
local DataOperationQueues = {}
local ChangedCallbacks = {}

----------------------------------------------------------------------------------------------------------------------------------------------------------------------
-- Helper methods
----------------------------------------------------------------------------------------------------------------------------------------------------------------------
local function RetryOperation(Operation, RetryAmount, RetryInterval, OperationDescription)
	DataService:DebugLog(("[Data Service] Attempting to %s..."):format(OperationDescription))

	for TryCount = 1, RetryAmount do
		local Success, Result = pcall(Operation)

		if not Success then
			if TryCount ~= RetryAmount then
				DataService:Log(
					("[Data Service] Failed to %s : %s | RETRYING IN %s SECONDS!"):format(
						OperationDescription,
						Result,
						tostring(RetryInterval)
					),
					"Warning"
				)

				task.wait(RetryInterval)
			else
				DataService:Log(
					("[Data Service] Failed to %s : %s | MAX RETRIES REACHED, ABORTING!"):format(
						OperationDescription,
						Result
					),
					"Warning"
				)
			end
		else
			DataService:DebugLog("[Data Service] " .. OperationDescription .. " succeeded!")

			return true, Result
		end
	end

	return false
end

local function AreQueuesEmpty()
	for _, OperationsQueue in pairs(DataOperationQueues) do
		if OperationsQueue:IsExecuting() then
			return false
		end
	end

	return true
end

local function GetSaveStore()
	return DatastoreService:GetDataStore(DATASTORE_NAME)
end

local function CreateSaveData(Player)
	return {
		CreatedTime = DateTime.now(),
		UpdatedTime = DateTime.now(),
		Version = 1,
		UserIDs = { Player.UserId },
		Data = Table.Copy(DATA_SCHEMA.Data, true),
		Metadata = { SchemaVersion = DATA_SCHEMA.Version },
		IsTemporary = true, --! This should ALWAYS be defined as true here - we don't want the data to be savable unless NO operations have failed. Read-only is a safe default for critical data.
	}
end

local function GetSessionLock(Player)
	local SaveStore = GetSaveStore()
	local Success, SessionLock = RetryOperation(
		function()
			local KeyValue = SaveStore:GetAsync(tostring(Player.UserId) .. "/SessionLock")

			return KeyValue
		end,
		OPERATION_MAX_RETRIES,
		OPERATION_RETRY_INTERVAL,
		("fetch sessionlock for player with ID '%s'"):format(tostring(Player.UserId))
	)

	return Success, SessionLock
end

local function WriteSessionLock(Player, SessionID)
	local Success = RetryOperation(
		function()
			local SaveStore = GetSaveStore()

			SaveStore:SetAsync(tostring(Player.UserId) .. "/SessionLock", SessionID, { Player.UserId })
		end,
		OPERATION_MAX_RETRIES,
		OPERATION_RETRY_INTERVAL,
		("write sessionlock for player with ID '%s'"):format(tostring(Player.UserId))
	)

	return Success
end

local function RemoveSessionLock(Player)
	local Success = RetryOperation(
		function()
			local SaveStore = GetSaveStore()

			SaveStore:RemoveAsync(tostring(Player.UserId) .. "/SessionLock")
		end,
		OPERATION_MAX_RETRIES,
		OPERATION_RETRY_INTERVAL,
		("remove sessionlock for player with ID '%s'"):format(tostring(Player.UserId))
	)

	return Success
end

local function FetchDataFromStore(Player)
	local Success, FetchedSaveData = RetryOperation(
		function()
			local SaveStore = GetSaveStore()
			local KeyData, KeyInfo = SaveStore:GetAsync(tostring(Player.UserId) .. "/SaveData")
			local SaveData = CreateSaveData(Player)

			if KeyData ~= nil then
				SaveData.Data = KeyData
				SaveData.CreatedTime = KeyInfo.CreatedTime
				SaveData.UpdatedTime = KeyInfo.UpdatedTime
				SaveData.Version = KeyInfo.Version
				SaveData.Metadata = KeyInfo:GetMetadata()
				SaveData.UserIDs = KeyInfo:GetUserIds()
			else
				DataService:DebugLog(
					("[Data Service] Data does not exist for player '%s', they may be a new player! Giving default data."):format(
						tostring(Player.UserId)
					)
				)
			end

			return SaveData
		end,
		OPERATION_MAX_RETRIES,
		OPERATION_RETRY_INTERVAL,
		("fetch data for player with ID '%s'"):format(tostring(Player.UserId))
	)

	return Success, FetchedSaveData
end

local function WriteDataToStore(Player, SaveData)
	if SaveData.IsTemporary then
		DataService:Log(
			("[Data Service] Player '%s' had temporary session-only data, aborting save!"):format(
				tostring(Player.UserId)
			),
			"Warning"
		)

		return true
	end

	local Success = RetryOperation(
		function()
			local SaveStore = GetSaveStore()
			local SetOptions = Instance.new("DataStoreSetOptions")

			SetOptions:SetMetadata(SaveData.Metadata)
			SaveStore:SetAsync(tostring(Player.UserId) .. "/SaveData", SaveData.Data, { Player.UserId }, SetOptions)
		end,
		OPERATION_MAX_RETRIES,
		OPERATION_RETRY_INTERVAL,
		("save data for player with ID '%s'"):format(tostring(Player.UserId))
	)

	return Success
end

local function PlayerAdded(Player)
	local CurrentSessionID = HttpService:GenerateGUID(false)
	local OperationsQueue

	Player:SetAttribute("SaveSessionID", CurrentSessionID)

	DataService:Log(
		("[Data Service] Player '%s' has joined, queued caching their savedata..."):format(tostring(Player.UserId))
	)

	---------------------------------------------
	-- Creating player's data operations queue --
	---------------------------------------------
	if DataOperationQueues[tostring(Player.UserId)] == nil then
		OperationsQueue = Queue.new()
		DataOperationQueues[tostring(Player.UserId)] = OperationsQueue

		DataService:DebugLog(
			("[Data Service] Created data operations queue for player '%s'."):format(tostring(Player.UserId))
		)
	else
		OperationsQueue = DataOperationQueues[tostring(Player.UserId)]

		DataService:DebugLog(
			("[Data Service] Using existing data operations queue for player '%s'."):format(tostring(Player.UserId))
		)
	end

	OperationsQueue:AddAction(function()
		------------------------------------------------------------
		-- Waiting for sessionlock & setting one for this session --
		------------------------------------------------------------
		RetryOperation(
			function()
				local _, SessionLock = GetSessionLock(Player)

				if SessionLock ~= nil then
					error("Sessionlock still exists.")
				else
					return
				end
			end,
			OPERATION_MAX_RETRIES,
			OPERATION_RETRY_INTERVAL,
			("wait for sessionlock removal for player '%s'"):format(tostring(Player.UserId))
		)
		local LockSuccess = WriteSessionLock(Player, CurrentSessionID)

		--------------------------------------------------------------
		-- Fetching save data from datastore & migrating its schema --
		--------------------------------------------------------------
		local GetDataSuccess, SavedData = FetchDataFromStore(Player)
		local MigrationSuccess, MigrationError = pcall(function()
			if GetDataSuccess and SavedData.Metadata.SchemaVersion < DATA_SCHEMA.Version then
				DataService:DebugLog(
					("[Data Service] Player '%s' has an outdated data schema, migrating to latest..."):format(
						tostring(Player.UserId)
					)
				)

				for SchemaVersion = SavedData.Metadata.SchemaVersion, DATA_SCHEMA.Version - 1 do
					DataService:DebugLog(
						("[Data Service] Migrating data from schema %s to schema %s..."):format(
							SavedData.Metadata.SchemaVersion,
							DATA_SCHEMA.Version
						)
					)

					SavedData.Data = DATA_SCHEMA.Migrators[SchemaVersion .. " -> " .. SchemaVersion + 1](SavedData.Data)
				end

				SavedData.Metadata.SchemaVersion = DATA_SCHEMA.Version
			end
		end)

		if not MigrationSuccess then
			DataService:Log(
				("[Data Service] Failed to migrate data for player '%s' : %s"):format(
					tostring(Player.UserId),
					MigrationError
				),
				"Warning"
			)
		end

		--------------------------------
		-- Caching player's save data --
		--------------------------------
		if not GetDataSuccess then
			DataService:Log(
				("[Data Service] Couldn't fetch save data from datastore for player '%s', data will be temporary."):format(
					tostring(Player.UserId)
				),
				"Warning"
			)

			DataCaches[Player:GetAttribute("SaveSessionID")] = CreateSaveData(Player)
		elseif not MigrationSuccess then
			DataService:Log(
				("[Data Service] Couldn't migrate data schema for player '%s', data will be temporary."):format(
					tostring(Player.UserId)
				),
				"Warning"
			)

			DataCaches[Player:GetAttribute("SaveSessionID")] = CreateSaveData(Player)
		elseif not LockSuccess then
			DataService:Log(
				("[Data Service] Couldn't sessionlock data for player '%s', data will be temporary."):format(
					tostring(Player.UserId)
				),
				"Warning"
			)

			DataCaches[Player:GetAttribute("SaveSessionID")] = SavedData
		else
			DataService:Log(("[Data Service] Savedata cached for player '%s'!"):format(tostring(Player.UserId)))

			SavedData.IsTemporary = false
			DataCaches[Player:GetAttribute("SaveSessionID")] = SavedData
		end

		EVENTS.DataLoaded:FireClient(Player, CurrentSessionID)
	end)

	if not OperationsQueue:IsExecuting() then
		DataService:DebugLog(
			("[Data Service] Executing operations queue for player '%s'..."):format(tostring(Player.UserId))
		)
		OperationsQueue:Execute()
		DataService:DebugLog(
			("[Data Service] Operations queue for player '%s' has finished, preserving in cache for later save operations."):format(
				tostring(Player.UserId)
			)
		)
	end
end

local function PlayerRemoved(Player)
	local OperationsQueue

	DataService:Log(
		("[Data Service] Player '%s' has left, queued writing their savedata to datastores and removing their savedata cache..."):format(
			tostring(Player.UserId)
		)
	)

	---------------------------------------------
	-- Getting player's data operations queue --
	---------------------------------------------
	OperationsQueue = DataOperationQueues[tostring(Player.UserId)]

	DataService:DebugLog(
		("[Data Service] Using existing data operations queue for player '%s'."):format(tostring(Player.UserId))
	)

	OperationsQueue:AddAction(function()
		------------------------------------
		-- Writing save data to datastore --
		------------------------------------
		WriteDataToStore(Player, DataCaches[Player:GetAttribute("SaveSessionID")])

		-------------------------
		-- Clearing data cache --
		-------------------------
		DataCaches[Player:GetAttribute("SaveSessionID")] = nil
		DataService:Log(("[Data Service] Removed savedata cache for player '%s'!"):format(tostring(Player.UserId)))

		---------------------------
		-- Removing session lock --
		---------------------------
		RemoveSessionLock(Player)
	end)

	if not OperationsQueue:IsExecuting() then
		DataService:DebugLog(
			("[Data Service] Executing operations queue for player '%s'..."):format(tostring(Player.UserId))
		)
		OperationsQueue:Execute()
		DataService:DebugLog(
			("[Data Service] Operations queue for player '%s' has finished, destroying queue & removing from queue cache!"):format(
				tostring(Player.UserId)
			)
		)
	end
end

----------------------------------------------------------------------------------------------------------------------------------------------------------------------
-- API Methods
----------------------------------------------------------------------------------------------------------------------------------------------------------------------

----------------------------------------------------------------------------------------------------------------------------------------------------------------------
-- @Name : WriteData
-- @Description : Calls the specified writer function which writes the given data to the player's savedata
-- @Params : Instance <Player> "Player" - The player whose data should be modified
--           string "Writer" - The name of the writer function to call
--           Tuple "Args" - The arguments to pass to the specified writer function
----------------------------------------------------------------------------------------------------------------------------------------------------------------------
function DataService:WriteData(Player, Writer, ...)
	while true do
		if not Player:IsDescendantOf(game) then
			return
		elseif DataCaches[Player:GetAttribute("SaveSessionID")] ~= nil then
			break
		end
		task.wait()
	end

	local DataChanges = table.pack(DATA_WRITERS[Writer](DataCaches[Player:GetAttribute("SaveSessionID")].Data, ...))
	local DataName = DataChanges[1]
	DataChanges[1] = Player

	EVENTS.DataWritten:FireClient(Player, Writer, ...)

	if ChangedCallbacks[DataName] ~= nil then
		for _, Callback in pairs(ChangedCallbacks[DataName]) do
			Callback(table.unpack(DataChanges))
		end
	end
end

----------------------------------------------------------------------------------------------------------------------------------------------------------------------
-- @Name : ReadData
-- @Description : Calls the specified reader function which reads the given player's savedata
-- @Params : Instance <Player> "Player" - The player whose data should be read from
--           string "Reader" - The name of the reader function to call
--           Tuple "Args" - The arguments to pass to the specified reader function
----------------------------------------------------------------------------------------------------------------------------------------------------------------------
function DataService:ReadData(Player, Reader, ...)
	while true do
		if not Player:IsDescendantOf(game) then
			return nil
		elseif DataCaches[Player:GetAttribute("SaveSessionID")] ~= nil then
			break
		end

		task.wait()
	end

	return DATA_READERS[Reader](Table.Copy(DataCaches[Player:GetAttribute("SaveSessionID")].Data, true), ...)
end

----------------------------------------------------------------------------------------------------------------------------------------------------------------------
-- @Name : OnDataChanged
-- @Description : Invokes the given callback when the specified data is changed
-- @Params : string "DataName" - The name of the data that should be listened to for changes
--           function "ChangedCallback" - The function to invoke when the specified data is changed
----------------------------------------------------------------------------------------------------------------------------------------------------------------------
function DataService:OnDataChanged(DataName, ChangedCallback)
	if ChangedCallbacks[DataName] ~= nil then
		table.insert(ChangedCallbacks[DataName], ChangedCallback)
	else
		ChangedCallbacks[DataName] = { ChangedCallback }
	end
end

----------------------------------------------------------------------------------------------------------------------------------------------------------------------
-- @Name : Configure
-- @Description : Sets the name of the datastore this service will use, as well as the schema & schema migration functions.
-- @Params : Table "Configs" - A table containing the configs for this service
----------------------------------------------------------------------------------------------------------------------------------------------------------------------
function DataService:Configure(Configs)
	DATASTORE_NAME = Configs.DatastoreName
	DATA_SCHEMA = Configs.Schema
	DATA_HANDLER_MODULE = Configs.DataHandlers
	DATA_WRITERS = require(Configs.DataHandlers).Writers
	DATA_READERS = require(Configs.DataHandlers).Readers
	WasConfigured = true
end

----------------------------------------------------------------------------------------------------------------------------------------------------------------------
-- @Name : Client.GetDataHandlerModule
-- @Description : Returns a reference to the data handler module to the calling client
----------------------------------------------------------------------------------------------------------------------------------------------------------------------
function DataService.Client:GetDataHandlerModule()
	return DATA_HANDLER_MODULE
end

----------------------------------------------------------------------------------------------------------------------------------------------------------------------
-- @Name : Client.RequestRawData
-- @Description : Returns the calling player's savedata to their client
----------------------------------------------------------------------------------------------------------------------------------------------------------------------
function DataService.Client:RequestRawData(Player)
	return DataCaches[Player:GetAttribute("SaveSessionID")].Data
end

----------------------------------------------------------------------------------------------------------------------------------------------------------------------
-- @Name : Init
-- @Description : Called when the service module is first loaded.
----------------------------------------------------------------------------------------------------------------------------------------------------------------------
function DataService:Init()
	self:DebugLog("[Data Service] Initializing...")

	EVENTS.DataWritten = self:RegisterServiceClientEvent("DataWritten")
	EVENTS.DataLoaded = self:RegisterServiceClientEvent("DataLoaded")

	if not WasConfigured then
		self:Log("[Data Service] The data service must be configured with Configure() before being used!", "Error")
	end

	game:BindToClose(function()
		self:Log("[Data Service] Server is shuting down, keeping it open so any cached data can save...")

		while true do
			if not AreQueuesEmpty() then
				task.wait()
			else
				break
			end
		end

		self:Log("[Data Service] Data cache is empty and operation queues are empty, allowing shutdown!")
	end)

	self:DebugLog("[Data Service] Initialized!")
end

----------------------------------------------------------------------------------------------------------------------------------------------------------------------
-- @Name : Start
-- @Description : Called after all services are loaded.
----------------------------------------------------------------------------------------------------------------------------------------------------------------------
function DataService:Start()
	self:DebugLog("[Data Service] Running!")

	for _, Player in pairs(Players:GetPlayers()) do
		task.spawn(PlayerAdded, Player)
	end
	Players.PlayerAdded:connect(PlayerAdded)
	Players.PlayerRemoving:connect(PlayerRemoved)
end

return DataService
