--!nocheck

local DataService = {}

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
local WasConfigured = false
local DataCaches = {}
local DataOperationQueues = {}

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

local function IsDataCacheEmpty()
	for _, _ in pairs(DataCaches) do
		return false
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

	DataService:Log(
		("[Data Service] Player '%s' has joined, caching their savedata..."):format(tostring(Player.UserId))
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

			DataCaches[tostring(Player.UserId)] = CreateSaveData(Player)
		elseif not MigrationSuccess then
			DataService:Log(
				("[Data Service] Couldn't migrate data schema for player '%s', data will be temporary."):format(
					tostring(Player.UserId)
				),
				"Warning"
			)

			DataCaches[tostring(Player.UserId)] = CreateSaveData(Player)
		elseif not LockSuccess then
			DataService:Log(
				("[Data Service] Couldn't sessionlock data for player '%s', data will be temporary."):format(
					tostring(Player.UserId)
				),
				"Warning"
			)

			DataCaches[tostring(Player.UserId)] = SavedData
		else
			DataService:Log(("[Data Service] Savedata cached for player '%s'!"):format(tostring(Player.UserId)))

			SavedData.IsTemporary = false
			DataCaches[tostring(Player.UserId)] = SavedData
		end

		print("[Data]", DataCaches[tostring(Player.UserId)])
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
		("[Data Service] Player '%s' has left, writing their savedata to datastores and removing their cache..."):format(
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
		WriteDataToStore(Player, DataCaches[tostring(Player.UserId)])

		-------------------------
		-- Clearing data cache --
		-------------------------
		DataCaches[tostring(Player.UserId)] = nil

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
-- @Name : Configure
-- @Description : Sets the name of the datastore this service will use, as well as the schema & schema migration functions.
-- @Paarams : Table "Configs" - A table containing the configs for this service
----------------------------------------------------------------------------------------------------------------------------------------------------------------------
function DataService:Configure(Configs)
	DATASTORE_NAME = Configs.DatastoreName
	DATA_SCHEMA = Configs.Schema
	WasConfigured = true
end

----------------------------------------------------------------------------------------------------------------------------------------------------------------------
-- @Name : Init
-- @Description : Called when the service module is first loaded.
----------------------------------------------------------------------------------------------------------------------------------------------------------------------
function DataService:Init()
	self:DebugLog("[Data Service] Initializing...")

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
