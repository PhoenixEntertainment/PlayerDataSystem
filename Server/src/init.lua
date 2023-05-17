--[[
	Data Service

	Handles the loading, saving and management of player data

	Backup system algorithm by @berezaa, modified and adapted by @Reshiram110
--]]

local DataService={Client={}}
DataService.Client.Server=DataService

---------------------
-- Roblox Services --
---------------------
local ReplicatedStorage = game:GetService("ReplicatedStorage")
local Players = game:GetService("Players")
local RunService = game:GetService("RunService")
local DatastoreService = game:GetService("DataStoreService")

------------------
-- Dependencies --
------------------
local RobloxLibModules = require(script.Parent["roblox-libmodules"])
local Table = require(RobloxLibModules.Utils.Table)
local Queue = require(RobloxLibModules.Classes.Queue)

-------------
-- Defines --
-------------
local DATASTORE_BASE_NAME = "Production" --The base name of the datastore to use
local DATASTORE_PRECISE_NAME = "PlayerData1" --The name of the datastore to append to DATASTORE_BASE_NAME
local DATASTORE_RETRY_ENABLED = true --Determines whether or not failed datastore calls will be retried
local DATASTORE_RETRY_INTERVAL = 3 --The time (in seconds) to wait between each retry
local DATASTORE_RETRY_LIMIT = 2 --The max amount of retries an operation can be retried before failing
local SESSION_LOCK_YIELD_INTERVAL = 5 -- The time (in seconds) at which the server will re-check a player's data session-lock.
                                      --! The interval should not be below 5 seconds, since Roblox caches keys for 4 seconds.
local SESSION_LOCK_MAX_YIELD_INTERVALS = 5 -- The maximum amount of times the server will re-check a player's session-lock before ignoring it
local DataFormat = {_FormatVersion = 1}
local DataFormatConversions = {}
local DataOperationsQueues = {}
local DataLoaded_IDs = {}
local DataCache = Instance.new('Folder') --Holds data for all players in ValueObject form
DataCache.Name = "_DataCache"
DataCache.Parent = ReplicatedStorage

------------
-- Events --
------------
local DataError; --Fired on the server when there is an error handling the player's data
local DataCreated; --Fired on the server when new data is created for a player.
local DataLoaded; -- Fired to the client when its data is loaded into the server's cache

----------------------------------------------------------------------------------------------------------------------------------------------------------------------
-- Helper functions
----------------------------------------------------------------------------------------------------------------------------------------------------------------------
local function GetOperationsQueue(Player)
	return DataOperationsQueues[tostring(Player.UserId)]
end

local function GetTotalQueuesSize()
	local QueuesSize = 0

	for _,OperationsQueue in pairs(DataOperationsQueues) do
		QueuesSize = QueuesSize + OperationsQueue:GetSize()
	end

	return QueuesSize
end

local function CreateDataCache(Player,Data,CanSave)
	local DataFolder = Table.ConvertTableToFolder(Data)
	DataFolder.Name = tostring(Player.UserId)

	DataFolder:SetAttribute("CanSave",CanSave)
	DataFolder.Parent = DataCache

	DataService:DebugLog(
		("[Data Service] Created data cache for player '%s', CanSave = %s!"):format(Player.Name,tostring(CanSave))
	)
end

local function RemoveDataCache(Player)
	local DataFolder = DataCache[tostring(Player.UserId)]
	
	DataFolder:Destroy()

	DataService:DebugLog(
		("[Data Service] Removed data cache for player '%s'!"):format(Player.Name)
	)
end

----------------------------------------------------------------------------------------------------------------------------------------------------------------------
-- @Name : IsDataLoaded
-- @Description : Returns a bool describing whether or not the specified player's data is loaded on the server or not.
-- @Params : Instance <Player> 'Player' - The player to check the data of
-- @Returns : bool "IsLoaded" - A bool describing whether or not the player's data is loaded on the server or not.
----------------------------------------------------------------------------------------------------------------------------------------------------------------------
function DataService:IsDataLoaded(Player)

	----------------
	-- Assertions --
	----------------
	assert(
		typeof(Player) == "Instance", 
		("[Data Service](IsDataLoaded) Bad argument #1 to 'GetData', Instance 'Player' expected, got %s instead.")
		:format(typeof(Player))
	)
	assert(
		Player:IsA("Player"),
		("[Data Service](IsDataLoaded) Bad argument #1 to 'GetData', Instance 'Player' expected, got Instance '%s' instead.")
		:format(Player.ClassName)
	)

	return DataCache:FindFirstChild(tostring(Player.UserId)) ~= nil
end

----------------------------------------------------------------------------------------------------------------------------------------------------------------------
-- @Name : GetData
-- @Description : Returns the data for the specified player and returns it in the specified format
-- @Params : Instance <Player> 'Player' - The player to get the data of
--           OPTIONAL string "Format" - The format to return the data in. Acceptable formats are "Table" and "Folder".
--           OPTIONAL bool "ShouldYield" - Whether or not the API should wait for the data to be fully loaded
-- @Returns : <Variant> "Data" - The player's data
----------------------------------------------------------------------------------------------------------------------------------------------------------------------
function DataService:GetData(Player,ShouldYield,Format)

	----------------
	-- Assertions --
	----------------
	assert(
		typeof(Player) == "Instance", 
		("[Data Service](GetData) Bad argument #1 to 'GetData', Instance 'Player' expected, got %s instead.")
		:format(typeof(Player))
	)
	assert(
		Player:IsA("Player"),
		("[Data Service](GetData) Bad argument #1 to 'GetData', Instance 'Player' expected, got Instance '%s' instead.")
		:format(Player.ClassName)
	)
	if ShouldYield ~= nil then
		assert(
			typeof(ShouldYield) == "boolean",
			("[Data Service](GetData) Bad argument #2 to 'GetData', bool expected, got %s instead.")
			:format(typeof(ShouldYield))
		)
	end
	if Format ~= nil then
		assert(
			typeof(Format) == "string",
			("[Data Service](GetData) Bad argument #3 to 'GetData', string expected, got %s instead.")
			:format(typeof(Format))
		)
		assert(
			string.upper(Format) == "FOLDER" or string.upper(Format) == "TABLE",
			("[Data Service](GetData) Bad argument #3 to 'GetData', invalid format. Valid formats are 'Table' or 'Folder', got '%s' instead.")
			:format(Format)
		)
	end

	local DataFolder = DataCache:FindFirstChild(tostring(Player.UserId))

	if DataFolder == nil then --Player's data did not exist
		if not ShouldYield then
			self:Log(
				("[Data Service](GetData) Failed to get data for player '%s', their data did not exist!"):format(Player.Name),
				"Warning"
			)

			return nil
		else
			DataFolder = DataCache:WaitForChild(tostring(Player.UserId))
		end
	end

	if Format == nil then
		return DataFolder
	elseif string.upper(Format) == "TABLE" then
		return Table.ConvertFolderToTable(DataFolder)
	elseif string.upper(Format) == "FOLDER" then
		return DataFolder
	end
end

----------------------------------------------------------------------------------------------------------------------------------------------------------------------
-- @Name : Client.GetDataLoadedQueueID
-- @Description : Fetches & returns the unique queue ID associated with the queue action that loads the data for the client
----------------------------------------------------------------------------------------------------------------------------------------------------------------------
function DataService.Client:GetDataLoadedQueueID(Player)
	return DataLoaded_IDs[tostring(Player.UserId)]
end

----------------------------------------------------------------------------------------------------------------------------------------------------------------------
-- @Name : Client.GetDataFolderDescendantCount
-- @Description : Returns the number of descendants in the calling player's data folder
----------------------------------------------------------------------------------------------------------------------------------------------------------------------
function DataService.Client:GetDataFolderDescendantCount(Player)
	return #self.Server:GetData(Player):GetDescendants()
end

----------------------------------------------------------------------------------------------------------------------------------------------------------------------
-- @Name : IsDataSessionlocked
-- @Description : Returns whether or not a player's data is session locked to another server
-- @Params : Instance <Player> 'Player' - The player to check the session lock status of
--           string "DatastoreName" - The name of the datastore to check the session lock in
-- @Returns : bool "OperationSucceeded" - A bool describing if the operation was successful or not
--            string "OperationMessage" - A message describing the result of the operation. can contain errors if the
--                                        operation fails.
--            bool "IsSessionlocked" - A bool describing whether or not the player's data is session-locked in another server.
----------------------------------------------------------------------------------------------------------------------------------------------------------------------
function DataService:IsDataSessionlocked(Player,DatastoreName)
	----------------
	-- Assertions --
	----------------
	assert(
		typeof(Player) == "Instance", 
		("[Data Service](IsDataSessionlocked) Bad argument #1 to 'IsDataSessionlocked', Instance 'Player' expected, got %s instead.")
		:format(typeof(Player))
	)
	assert(
		Player:IsA("Player"),
		("[Data Service](IsDataSessionlocked) Bad argument #1 to 'IsDataSessionlocked', Instance 'Player' expected, got Instance '%s' instead.")
		:format(Player.ClassName)
	)
	assert(
		typeof(DatastoreName) == "string",
		("[Data Service](IsDataSessionlocked) Bad argument #2 to 'IsDataSessionlocked', string expected, got %s instead.")
		:format(typeof(DatastoreName))
	)

	self:DebugLog(
		("[Data Service](IsDataSessionlocked) Getting session lock for %s in datastore '%s'...")
		:format(Player.Name,DATASTORE_BASE_NAME .. "_" .. DatastoreName)
	)

	local SessionLock_Datastore = DatastoreService:GetDataStore(
		DATASTORE_BASE_NAME .. "_" .. DatastoreName .. "_SessionLocks",
		tostring(Player.UserId)
	)
	local SessionLocked = false

	local GetSessionLock_Success,GetSessionLock_Error = pcall(function()
		SessionLocked = SessionLock_Datastore:GetAsync("SessionLock")
	end)

	if GetSessionLock_Success then
		self:DebugLog(
			("[Data Service](IsDataSessionLocked) Got session lock for %s!")
			:format(Player.Name)
		)

		return true,"Operation Success",SessionLocked
	else
		self:Log(
			("[Data Service](IsDataSessionlocked) An error occured while reading session-lock for '%s' : Could not read session-lock, %s")
			:format(Player.Name,GetSessionLock_Error),
			"Warning"
		)

		return false,"Failed to read session-lock : Could not read session-lock, " .. GetSessionLock_Error
	end
end

----------------------------------------------------------------------------------------------------------------------------------------------------------------------
-- @Name : SessionlockData
-- @Description : Locks the data for the specified player to the current server
-- @Params : Instance <Player> 'Player' - The player to session lock the data of
--           string "DatastoreName" - The name of the datastore to lock the data in
-- @Returns : bool "OperationSucceeded" - A bool describing if the operation was successful or not
--            string "OperationMessage" - A message describing the result of the operation. Can contain errors if the
--                                        operation fails.
----------------------------------------------------------------------------------------------------------------------------------------------------------------------
function DataService:SessionlockData(Player,DatastoreName)
	----------------
	-- Assertions --
	----------------
	assert(
		typeof(Player) == "Instance", 
		("[Data Service](SessionlockData) Bad argument #1 to 'SessionlockData', Instance 'Player' expected, got %s instead.")
		:format(typeof(Player))
	)
	assert(
		Player:IsA("Player"),
		("[Data Service](SessionlockData) Bad argument #1 to 'SessionlockData', Instance 'Player' expected, got Instance '%s' instead.")
		:format(Player.ClassName)
	)
	assert(
		typeof(DatastoreName) == "string",
		("[Data Service](SessionlockData) Bad argument #2 to 'SessionlockData', string expected, got %s instead.")
		:format(typeof(DatastoreName))
	)

	self:DebugLog(
		("[Data Service](SessionlockData) Locking data for %s in datastore '%s'..."):format(Player.Name,DATASTORE_BASE_NAME.."_"..DatastoreName)
	)

	local SessionLock_Datastore = DatastoreService:GetDataStore(
		DATASTORE_BASE_NAME .. "_" .. DatastoreName .. "_SessionLocks",
		tostring(Player.UserId)
	)

	local WriteLock_Success,WriteLock_Error = pcall(function()
		SessionLock_Datastore:SetAsync("SessionLock",true)
	end)

	if WriteLock_Success then
		self:DebugLog(
			("[Data Service](SessionlockData) Locked data for %s!")
			:format(Player.Name)
		)

		return true,"Operation Success"
	else
		self:Log(
			("[Data Service](SessionlockData) An error occured while session-locking data for '%s' : Could not write session-lock, %s")
			:format(Player.Name,WriteLock_Error),
			"Warning"
		)

		return false,"Failed to session-lock data : Could not write session-lock, " .. WriteLock_Error
	end
end

----------------------------------------------------------------------------------------------------------------------------------------------------------------------
-- @Name : UnSessionlockData
-- @Description : Unlocks the data for the specified player from the current server
-- @Params : Instance <Player> 'Player' - The player to un-session lock the data of
--           string "DatastoreName" - The name of the datastore to un-lock the data in
-- @Returns : bool "OperationSucceeded" - A bool describing if the operation was successful or not
--            string "OperationMessage" - A message describing the result of the operation. Can contain errors if the
--                                        operation fails.
----------------------------------------------------------------------------------------------------------------------------------------------------------------------
function DataService:UnSessionlockData(Player,DatastoreName)
	----------------
	-- Assertions --
	----------------
	assert(
		typeof(Player) == "Instance", 
		("[Data Service](UnSessionlockData) Bad argument #1 to 'UnSessionlockData', Instance 'Player' expected, got %s instead.")
		:format(typeof(Player))
	)
	assert(
		Player:IsA("Player"),
		("[Data Service](UnSessionlockData) Bad argument #1 to 'UnSessionlockData', Instance 'Player' expected, got Instance '%s' instead.")
		:format(Player.ClassName)
	)
	assert(
		typeof(DatastoreName) == "string",
		("[Data Service](UnSessionlockData) Bad argument #2 to 'UnSessionlockData', string expected, got %s instead.")
		:format(typeof(DatastoreName))
	)

	self:DebugLog(
		("[Data Service](UnSessionlockData) Unlocking data for %s in datastore '%s'..."):format(Player.Name,DATASTORE_BASE_NAME.."_"..DatastoreName)
	)

	local SessionLock_Datastore = DatastoreService:GetDataStore(
		DATASTORE_BASE_NAME .. "_" .. DatastoreName .. "_SessionLocks",
		tostring(Player.UserId)
	)

	local WriteLock_Success,WriteLock_Error = pcall(function()
		SessionLock_Datastore:SetAsync("SessionLock",false)
	end)

	if WriteLock_Success then
		self:DebugLog(
			("[Data Service](UnSessionlockData) Unlocked data for %s!")
			:format(Player.Name)
		)

		return true,"Operation Success"
	else
		self:Log(
			("[Data Service](UnSessionlockData) An error occured while un-session-locking data for '%s' : Could not write session-lock, %s")
			:format(Player.Name,WriteLock_Error),
			"Warning"
		)

		return false,"Failed to session-lock data : Could not write session-lock, " .. WriteLock_Error
	end
end

----------------------------------------------------------------------------------------------------------------------------------------------------------------------
-- @Name : LoadData
-- @Description : Loads the data for the specified player and returns it as a table
-- @Params : Instance <Player> 'Player' - The player to load the data of
--           string "DatastoreName" - The name of the datastore to load the data from
-- @Returns : bool "OperationSucceeded" - A bool describing if the operation was successful or not
--            string "OperationMessage" - A message describing the result of the operation. Can contain errors if the
--                                        operation fails.
--            table "Data" - The player's data. Will be nil if the operation fails.
----------------------------------------------------------------------------------------------------------------------------------------------------------------------
function DataService:LoadData(Player,DatastoreName)

	----------------
	-- Assertions --
	----------------
	assert(
		typeof(Player) == "Instance", 
		("[Data Service](LoadData) Bad argument #1 to 'SaveData', Instance 'Player' expected, got %s instead.")
		:format(typeof(Player))
	)
	assert(
		Player:IsA("Player"),
		("[Data Service](LoadData) Bad argument #1 to 'SaveData', Instance 'Player' expected, got Instance '%s' instead.")
		:format(Player.ClassName)
	)
	assert(
		typeof(DatastoreName) == "string",
		("[Data Service](LoadData) Bad argument #2 to 'SaveData', string expected, got %s instead.")
		:format(typeof(DatastoreName))
	)

	self:DebugLog(
		("[Data Service](LoadData) Loading data for %s from datastore '%s'..."):format(Player.Name,DATASTORE_BASE_NAME.."_"..DatastoreName)
	)

	-------------
	-- Defines --
	-------------
	local Data_Datastore = DatastoreService:GetDataStore(DATASTORE_BASE_NAME.."_"..DatastoreName.."_Data",tostring(Player.UserId))
	local Pointer_Datastore = DatastoreService:GetOrderedDataStore(DATASTORE_BASE_NAME.."_"..DatastoreName.."_DataPointers",tostring(Player.UserId))
	local Data_VersionNumber; --Holds the current version number of the data
	local Data = Table.Copy(DataFormat) --Holds the player's data

	-------------------------------------------
	-- Fetching previous data version number --
	-------------------------------------------
	local GetDataVersionSuccess,GetDataVersionErrorMessage = pcall(function()
		local Pages = Pointer_Datastore:GetSortedAsync(false,1)
		local LatestKey = Pages:GetCurrentPage()[1]

		if LatestKey ~= nil then
			Data_VersionNumber = LatestKey.value
		end
	end)
	if not GetDataVersionSuccess then --! An error occured while getting the player's data version number
		self:Log(
			("[Data Service](LoadData) An error occured while loading data for '%s' : Failed to get data version number, %s")
			:format(Player.Name,GetDataVersionErrorMessage),
			"Warning"
		)

		Data = Table.Copy(DataFormat)
		DataError:Fire(Player,"Load","FetchDataVersion",Data)
		self.Client.DataError:FireAllClients(Player,"Load","FetchDataVersion",Data)

		return false,"Failed to load data : Unable to get data version number, "..GetDataVersionErrorMessage,Data
	else --Data version number fetched successfully, check if new data is being created
		if Data_VersionNumber == nil then --* It is the first time loading data from this datastore. Player must be new!
			self:DebugLog(
				("[Data Service](LoadData) Data created for the first time for player '%s', they may be new!"):format(Player.Name)
			)

			DataCreated:Fire(Player,Data)
			self.Client.DataCreated:FireAllClients(Player,Data)

			return true,"Operation Success",Data
		end	
	end

	-------------------------------------------------
	-- Loading player's data from normal datastore --
	-------------------------------------------------
	local GetDataSuccess,GetDataErrorMessage = pcall(function()
		Data = Data_Datastore:GetAsync(tostring(Data_VersionNumber))

		if Data == nil then
			error("Data version " .. Data_VersionNumber .. " was not found.")
		end
	end)
	if not GetDataSuccess then --! An error occured while getting the player's data
		self:Log(
			("[Data Service](LoadData) An error occured while loading data for player '%s' : %s")
			:format(Player.Name,GetDataErrorMessage),
			"Warning"
		)

		Data = Table.Copy(DataFormat)
		DataError:Fire(Player,"Load","FetchData",Data)
		self.Client.DataError:FireAllClients(Player,"Load","FetchData",Data)

		return false,"Failed to load data : "..GetDataErrorMessage,Data
	end

	------------------------------------------
	-- Updating the data's format if needed --
	------------------------------------------
	if Data._FormatVersion < DataFormat._FormatVersion then --Data format is outdated, it needs to be updated.
		self:DebugLog(
			("[Data Service](LoadData) %s's data format is oudated, updating..."):format(Player.Name)
		)

		local DataFormatUpdateSuccess,DataFormatUpdateErrorMessage = pcall(function()
			for _ = Data._FormatVersion,DataFormat._FormatVersion - 1 do
				self:DebugLog(
					("[Data Service] Updating %s's data from version %s to version %s...")
					:format(Player.Name,tostring(Data._FormatVersion),tostring(Data._FormatVersion + 1))
				)

				Data = DataFormatConversions[tostring(Data._FormatVersion).." -> "..tostring(Data._FormatVersion+1)](Data)
				Data._FormatVersion = Data._FormatVersion + 1
			end
		end)

		if not DataFormatUpdateSuccess then --! An error occured while updating the player's data
			self:Log(
				("[Data Service](LoadData) An error occured while updating the data for player '%s' : %s")
				:format(Player.Name,DataFormatUpdateErrorMessage),
				"Warning"
			)

			Data = Table.Copy(DataFormat)
			DataError:Fire(Player,"Load","FormatUpdate",Data)
			self.Client.DataError:FireAllClients(Player,"Load","FormatUpdate",Data)

			return false,"Failed to load data : Update failed, "..DataFormatUpdateErrorMessage,Data
		end
	elseif Data._FormatVersion == nil or Data._FormatVersion > DataFormat._FormatVersion then -- Unreadable data format, do not load data.
		self:Log(
			("[Data Service](LoadData) An error occured while loading the data for player '%s' : %s")
			:format(Player.Name,"Unknown data format"),
			"Warning"
		)

		Data = Table.Copy(DataFormat)
		DataError:Fire(Player,"Load","UnknownDataFormat",Data)

		self.Client.DataError:FireAllClients(Player,"Load","UnknownDatFormat",Data)

		return false,"Failed to load data : Unknown data format",Data
	end

	self:DebugLog(
		("[Data Service](LoadData) Successfully loaded data for player '%s'!"):format(Player.Name)
	)

	return true,"Operation Success",Data
end

----------------------------------------------------------------------------------------------------------------------------------------------------------------------
-- @Name : SaveData
-- @Description : Saves the data for the specified player into the specified datastore
-- @Params : Instance <Player> 'Player' - the player to save the data of
--           string "DatastoreName" - The name of the datastore to save the data to
--           table "Data" - The table containing the data to save
-- @Returns : bool "OperationSucceeded" - A bool describing if the operation was successful or not
--            string "OperationMessage" - A message describing the result of the operation. Can contain errors if the
--                                        operation fails.
----------------------------------------------------------------------------------------------------------------------------------------------------------------------
function DataService:SaveData(Player,DatastoreName,Data)

	----------------
	-- Assertions --
	----------------
	assert(
		typeof(Player) == "Instance", 
		("[Data Service](SaveData) Bad argument #1 to 'SaveData', Instance 'Player' expected, got %s instead.")
		:format(typeof(Player))
	)
	assert(
		Player:IsA("Player"),
		("[Data Service](SaveData) Bad argument #1 to 'SaveData', Instance 'Player' expected, got Instance '%s' instead.")
		:format(Player.ClassName)
	)
	assert(
		typeof(DatastoreName) == "string",
		("[Data Service](SaveData) Bad argument #2 to 'SaveData', string expected, got %s instead.")
		:format(typeof(DatastoreName))
	)
	assert(
		Data ~= nil,
		"[Data Service](SaveData) Bad argument #3 to 'SaveData', Data expected, got nil."
	)

	self:DebugLog(
		("[Data Service](SaveData) Saving data for %s into datastore '%s'..."):format(Player.Name,DATASTORE_BASE_NAME.."_"..DatastoreName)
	)

	-------------
	-- Defines --
	-------------
	local Data_Datastore = DatastoreService:GetDataStore(DATASTORE_BASE_NAME.."_"..DatastoreName.."_Data",tostring(Player.UserId))
	local Pointer_Datastore = DatastoreService:GetOrderedDataStore(DATASTORE_BASE_NAME.."_"..DatastoreName.."_DataPointers",tostring(Player.UserId))
	local Data_VersionNumber; --Holds the current version number of the data

	-------------------------------------------
	-- Fetching previous data version number --
	-------------------------------------------
	local GetDataVersionSuccess,GetDataVersionErrorMessage = pcall(function()
		local Pages = Pointer_Datastore:GetSortedAsync(false,1)
		local LatestKey = Pages:GetCurrentPage()[1]
		local VersionNumber;

		if LatestKey ~= nil then
			VersionNumber = LatestKey.value
		end
		if VersionNumber == nil then --* It is the first time saving the data to this datastore
			Data_VersionNumber = 1
		else
			Data_VersionNumber = VersionNumber + 1
		end	
	end)
	if not GetDataVersionSuccess then --! An error occured while getting the player's data version number
		self:Log(
			("[Data Service](SaveData) An error occured while saving data for '%s' : Failed to get data version number, %s")
			:format(Player.Name,GetDataVersionErrorMessage),
			"Warning"
		)

		DataError:Fire(Player,"Save","FetchDataVersion",Data)
		self.Client.DataError:FireAllClients(Player,"Save","FetchDataVersion",Data)

		return false,"Failed to save data : Unable to get data version number, "..GetDataVersionErrorMessage
	end

	----------------------------------------------
	-- Saving player's data to normal datastore --
	----------------------------------------------
	local SaveDataSuccess,SaveDataErrorMessage = pcall(function()
		Data_Datastore:SetAsync(tostring(Data_VersionNumber),Data)
	end)
	if not SaveDataSuccess then --! An error occured while saving the player's data.
		self:Log(
			("[Data Service](SaveData) An error occured while saving data for '%s' : %s"):format(Player.Name,SaveDataErrorMessage),
			"Warning"
		)

		DataError:Fire(Player,"Save","SaveData",Data)
		self.Client.DataError:FireAllClients(Player,"Save","SaveData",Data)

		return false,"Failed to save data : "..SaveDataErrorMessage
	end

	-----------------------------------------------------
	-- Saving data version number to ordered datastore --
	-----------------------------------------------------
	local SaveVersionNumberSuccess,SaveVersionNumberErrorMessage = pcall(function()
		Pointer_Datastore:SetAsync(tostring(os.time()),Data_VersionNumber)
	end)
	if not SaveVersionNumberSuccess then --! An error occured while saving the data's version number
		self:Log(
			("[Data Service](SaveData) An error occured while saving data for '%s', failed to save data version number : %s")
			:format(Player.Name,SaveVersionNumberErrorMessage),
			"Warning"
		)

		DataError:Fire(Player,"Save","SaveDataVersion",Data)
		self.Client.DataError:FireAllClients(Player,"Save","SaveDataVersion",Data)

		return false,"Failed to save data : Unable to save data version number, "..SaveVersionNumberErrorMessage
	end

	self:DebugLog(
		("[Data Service](SaveData) Data saved successfully into datastore '%s' for %s!"):format(DATASTORE_BASE_NAME.."_"..DatastoreName,Player.Name)
	)

	return true,"Operation Success"
end

----------------------------------------------------------------------------------------------------------------------------------------------------------------------
-- @Name : SetConfigs
-- @Description : Sets this service's configs to the specified values
-- @Params : table "Configs" - A dictionary containing the new config values
----------------------------------------------------------------------------------------------------------------------------------------------------------------------
function DataService:SetConfigs(Configs)
	DataFormat = Configs.DataFormat
	DataFormatConversions = Configs.DataFormatConversions
	DATASTORE_BASE_NAME = Configs.DatastoreBaseName
	DATASTORE_PRECISE_NAME = Configs.DatastorePreciseName
	DATASTORE_RETRY_ENABLED = Configs.DatastoreRetryEnabled
	DATASTORE_RETRY_INTERVAL = Configs.DatastoreRetryInterval
	DATASTORE_RETRY_LIMIT = Configs.DatastoreRetryLimit
	SESSION_LOCK_YIELD_INTERVAL = Configs.SessionLockYieldInterval
	SESSION_LOCK_MAX_YIELD_INTERVALS = Configs.SessionLockMaxYieldIntervals
end

----------------------------------------------------------------------------------------------------------------------------------------------------------------------
-- @Name : Init
-- @Description : Called when the service module is first loaded.
----------------------------------------------------------------------------------------------------------------------------------------------------------------------
function DataService:Init()
	DataLoaded = self:RegisterServiceClientEvent("DataLoaded")
	DataCreated = self:RegisterServiceServerEvent("DataCreated")
	DataError = self:RegisterServiceServerEvent("DataError")
	self.Client.DataCreated = self:RegisterServiceClientEvent("DataCreated")
	self.Client.DataError = self:RegisterServiceClientEvent("DataError")

	self:DebugLog("[Data Service] Initialized!")
end

----------------------------------------------------------------------------------------------------------------------------------------------------------------------
-- @Name : Start
-- @Description : Called after all services are loaded.
----------------------------------------------------------------------------------------------------------------------------------------------------------------------
function DataService:Start()
	self:DebugLog("[Data Service] Started!")

	-------------------------------------------
	-- Loads player data into server's cache --
	-------------------------------------------
	local function LoadPlayerDataIntoServer(Player)
		local WaitForSessionLock_Success = false -- Determines whether or not the session lock was waited for successfully
		local SetSessionLock_Success = false -- Determines whether or not the session lock was successfully enabled for this server
		local LoadData_Success = false -- Determines whether or not the player's data was fetched successfully
		local PlayerData;

		self:Log(
			("[Data Service] Loading data for player '%s'..."):format(Player.Name)
		)

		----------------------------------------------------
		-- Waiting for other server's sessionlock removal --
		----------------------------------------------------
		self:DebugLog(
			("[Data Service] Waiting for previous server to remove session lock for player '%s'...")
			:format(Player.Name)
		)

		for SessionLock_YieldCount = 1,SESSION_LOCK_MAX_YIELD_INTERVALS do
			local GetLockSuccess;
			local OperationMessage;
			local IsLocked;

			--------------------------------
			-- Reading session lock value --
			--------------------------------
			for RetryCount = 0, DATASTORE_RETRY_LIMIT do
				self:DebugLog(
					("[Data Service] Reading session lock for player '%s'..."):format(Player.Name)
				)

				GetLockSuccess,OperationMessage,IsLocked = self:IsDataSessionlocked(Player,DATASTORE_PRECISE_NAME)

				if not GetLockSuccess then
					self:Log(
						("[Data Service] Failed to read session lock for player '%s' : %s"):format(Player.Name,OperationMessage),
						"Warning"
					)

					if RetryCount == DATASTORE_RETRY_LIMIT then
						self:Log(
							("[Data Service] Max retries reached while attempting to read session lock for player '%s', aborting")
							:format(Player.Name),
							"Warning"
						)

						break
					else
						if DATASTORE_RETRY_ENABLED then
							self:Log(
								("[Data Service] Attempting to read session lock for player '%s' %s more times.")
								:format(Player.Name,tostring(DATASTORE_RETRY_LIMIT - RetryCount))
							)

							task.wait(DATASTORE_RETRY_INTERVAL)
						else
							break
						end
					end
				else
					self:DebugLog(
						("[Data Service] Got session lock for player '%s'!"):format(Player.Name)
					)

					break
				end
			end

			--------------------------------------------
			-- Determining if sessionlock was removed --
			--------------------------------------------
			if not GetLockSuccess then
				break
			end

			if IsLocked then
				if SessionLock_YieldCount == SESSION_LOCK_MAX_YIELD_INTERVALS then
					self:Log(
						("[Data Service] Timeout reached while waiting for previous server to remove its sessionlock for player '%s', ignoring it.")
						:format(Player.Name),
						"Warning"
					)

					WaitForSessionLock_Success = true
				else
					self:DebugLog(
						("[Data Service] Previous server hasn't removed session lock for player '%s' yet, waiting %s seconds before re-reading.")
						:format(Player.Name, tostring(SESSION_LOCK_YIELD_INTERVAL))
					)
				end

				task.wait(SESSION_LOCK_YIELD_INTERVAL)
			else
				self:DebugLog(
					("[Data Service] Previous server removed session lock for player '%s'!"):format(Player.Name)
				)

				WaitForSessionLock_Success = true
				break
			end
		end

		--------------------------
		-- Setting session lock --
		--------------------------
		if not WaitForSessionLock_Success then
			self:Log(
				("[Data Service] Failed to set session lock to this server, giving player '%s' default data."):format(Player.Name),
				"Warning"
			)

			CreateDataCache(Player,Table.Copy(DataFormat),false)
			return
		else
			self:DebugLog(
				("[Data Service] Setting session-lock for player '%s'..."):format(Player.Name)
			)
		end

		for RetryCount = 1,DATASTORE_RETRY_LIMIT do
			self:DebugLog(
				("[Data Service] Writing sessionlock to datastore '%s' for player '%s'..."):format(DATASTORE_PRECISE_NAME,Player.Name)
			)

			local SetLockSuccess,SetLockMessage = self:SessionlockData(Player,DATASTORE_PRECISE_NAME)

			if not SetLockSuccess then
				self:Log(
					("[Data Service] Failed to set session-lock for player '%s' : %s")
					:format(Player.Name,SetLockMessage),
					"Warning"
				)

				if DATASTORE_RETRY_ENABLED then
					if RetryCount == DATASTORE_RETRY_LIMIT then
						self:Log(
							("[Data Service] Max retries reached while trying to session-lock data for player '%s', no further attempts will be made.")
							:format(Player.Name),
							"Warning"
						)
					else
						self:Log(
							("[Data Service] Retrying to session-lock data for player '%s', waiting %s seconds before retrying.")
							:format(Player.Name,tostring(DATASTORE_RETRY_INTERVAL)),
							"Warning"
						)

						task.wait(DATASTORE_RETRY_INTERVAL)
					end
				else
					break
				end
			else
				self:DebugLog(
					("[Data Service] Successfully session-locked data for player '%s'!"):format(Player.Name)
				)

				SetSessionLock_Success = true
				break
			end
		end

		----------------------------
		-- Fetching player's data --
		----------------------------
		if not SetSessionLock_Success then
			self:Log(
				("[Data Service] Failed to set session-lock, giving player '%s' default data."):format(Player.Name),
				"Warning"
			)

			CreateDataCache(Player,Table.Copy(DataFormat),false)
			return
		else
			self:DebugLog(
				("[Data Service] Fetching data for player '%s' from datastore..."):format(Player.Name)
			)
		end

		for RetryCount = 1,DATASTORE_RETRY_LIMIT do
			self:DebugLog(
				("[Data Service] Reading data from datastore '%s' for player '%s'...")
				:format(DATASTORE_PRECISE_NAME,Player.Name)
			)

			local FetchDataSuccess,FetchDataMessage,Data = self:LoadData(Player,DATASTORE_PRECISE_NAME)

			if not FetchDataSuccess then
				self:Log(
					("[Data Service] Failed to fetch data for player '%s' : %s")
					:format(Player.Name,FetchDataMessage),
					"Warning"
				)

				if DATASTORE_RETRY_ENABLED then
					if RetryCount == DATASTORE_RETRY_LIMIT then
						self:Log(
							("[Data Service] Max retries reached while trying to load data for player '%s', no further attempts will be made.")
							:format(Player.Name),
							"Warning"
						)
					else
						self:Log(
							("[Data Service] Retrying to fetch data for player '%s', waiting %s seconds before retrying.")
							:format(Player.Name,tostring(DATASTORE_RETRY_INTERVAL)),
							"Warning"
						)

						task.wait(DATASTORE_RETRY_INTERVAL)
					end
				else
					break
				end
			else
				self:DebugLog(
					("[Data Service] Successfully fetched data for player '%s' from datastores!"):format(Player.Name)
				)

				LoadData_Success = true
				PlayerData = Data
				break
			end
		end

		if not LoadData_Success then
			self:Log(
				("[Data Service] Failed to load data for player '%s', player will be given default data.")
				:format(Player.Name),
				"Warning"
			)

			CreateDataCache(Player,Table.Copy(DataFormat),false)
		else
			self:Log(
				("[Data Service] Successfully loaded data for player '%s'!"):format(Player.Name)
			)

			CreateDataCache(Player,PlayerData,true)
		end
	end

	-------------------------------------------
	-- Saves player data from servers' cache --
	-------------------------------------------
	local function SavePlayerDataFromServer(Player)
		local PlayerData = self:GetData(Player,false,"Table")
		local WriteData_Success = false -- Determines whether or not the player's data was successfully saved to datastores

		self:Log(
			("[Data Service] Saving data for player '%s'..."):format(Player.Name)
		)

		-------------------------------
		-- Writing data to datastore --
		-------------------------------
		self:DebugLog(
			("[Data Service] Writing data to datastores for player '%s'..."):format(Player.Name)
		)
		for RetryCount = 1,DATASTORE_RETRY_LIMIT do
			self:DebugLog(
				("[Data Service] Writing data to datastore '%s' for player '%s'...")
				:format(DATASTORE_PRECISE_NAME,Player.Name)
			)

			local WriteDataSuccess,WriteDataMessage = self:SaveData(Player,DATASTORE_PRECISE_NAME,PlayerData)

			if not WriteDataSuccess then
				self:Log(
					("[Data Service] Failed to write data for player '%s' : %s")
					:format(Player.Name,WriteDataMessage),
					"Warning"
				)

				if DATASTORE_RETRY_ENABLED then
					if RetryCount == DATASTORE_RETRY_LIMIT then
						self:Log(
							("[Data Service] Max retries reached while trying to write data for player '%s', no further attempts will be made.")
							:format(Player.Name),
							"Warning"
						)
					else
						self:Log(
							("[Data Service] Retrying to write data for player '%s', waiting %s seconds before retrying.")
							:format(Player.Name,tostring(DATASTORE_RETRY_INTERVAL)),
							"Warning"
						)

						task.wait(DATASTORE_RETRY_INTERVAL)
					end
				else
					break
				end
			else
				self:DebugLog(
					("[Data Service] Successfully wrote data for player '%s' to datastores!"):format(Player.Name)
				)

				WriteData_Success = true
				break
			end
		end

		if not WriteData_Success then
			self:Log(
				("[Data Service] Failed to save data for player '%s'."):format(Player.Name),
				"Warning"
			)
		else
			self:Log(
				("[Data Service] Successfully saved data for player '%s'!"):format(Player.Name)
			)
		end

		----------------------------
		-- Un-sessionlocking data --
		----------------------------
		self:DebugLog(
			("[Data Service] Un-session locking data for player '%s'..."):format(Player.Name)
		)

		for RetryCount = 1,DATASTORE_RETRY_LIMIT do
			self:DebugLog(
				("[Data Service] Removing sessionlock from datastore '%s' for player '%s'..."):format(DATASTORE_PRECISE_NAME,Player.Name)
			)

			local RemoveLockSuccess,RemoveLockMessage = self:UnSessionlockData(Player,DATASTORE_PRECISE_NAME)

			if not RemoveLockSuccess then
				self:Log(
					("[Data Service] Failed to remove session-lock for player '%s' : %s")
					:format(Player.Name,RemoveLockMessage),
					"Warning"
				)

				if DATASTORE_RETRY_ENABLED then
					if RetryCount == DATASTORE_RETRY_LIMIT then
						self:Log(
							("[Data Service] Max retries reached while trying to remove session-lock for player '%s', no further attempts will be made.")
							:format(Player.Name),
							"Warning"
						)
					else
						self:Log(
							("[Data Service] Retrying to remove session-lock for player '%s', waiting %s seconds before retrying.")
							:format(Player.Name,tostring(DATASTORE_RETRY_INTERVAL)),
							"Warning"
						)

						task.wait(DATASTORE_RETRY_INTERVAL)
					end
				else
					break
				end
			else
				self:DebugLog(
					("[Data Service] Successfully removed session-lock for player '%s'!"):format(Player.Name)
				)

				break
			end
		end

		RemoveDataCache(Player)
	end

	---------------------------------
	-- Loading player data on join --
	---------------------------------
	local function PlayerJoined(Player)
		if GetOperationsQueue(Player) == nil then
			DataOperationsQueues[tostring(Player.UserId)] = Queue.new()
		end

		local DataOperationsQueue = GetOperationsQueue(Player)

		local QueueItemID = DataOperationsQueue:AddAction(
			function()
				LoadPlayerDataIntoServer(Player)
			end,
			function(ActionID)
				DataLoaded:FireClient(Player,ActionID)
			end
		)
		DataLoaded_IDs[tostring(Player.UserId)] = QueueItemID

		if not DataOperationsQueue:IsExecuting() then
			DataOperationsQueue:Execute()
		end
	end
	Players.PlayerAdded:connect(PlayerJoined)
	for _,Player in pairs(Players:GetPlayers()) do
		coroutine.wrap(PlayerJoined)(Player)
	end

	---------------------------------
	-- Saving player data on leave --
	---------------------------------
	local function PlayerLeaving(Player)
		DataLoaded_IDs[tostring(Player.UserId)] = nil

		local DataOperationsQueue = GetOperationsQueue(Player)

		DataOperationsQueue:AddAction(
			function()
				if self:GetData(Player,false):GetAttribute("CanSave") == false then
					self:Log(
						("[Data Service] Player '%s' left, but their data was marked as not saveable. Will not save data."):format(Player.Name),
						"Warning"
					)

					RemoveDataCache(Player)
					
					return
				else
					SavePlayerDataFromServer(Player)
				end
			end,
			function()
				if DataOperationsQueue:GetSize() == 0 then
					DataOperationsQueue:Destroy()
					DataOperationsQueues[tostring(Player.UserId)] = nil
				end
			end
		)

		if not DataOperationsQueue:IsExecuting() then
			DataOperationsQueue:Execute()
		end
	end
	Players.PlayerRemoving:connect(PlayerLeaving)

	--------------------------------------------------------------------------------
	-- Ensuring that all player data is saved before letting the server shut down --
	--------------------------------------------------------------------------------
	game:BindToClose(function()
		self:Log("[Data Service] Server shutting down, waiting for data operations queue to be empty...")

		while true do -- Wait for all player data to be saved
			if GetTotalQueuesSize() == 0 then
				break
			end
			RunService.Stepped:wait()
		end

		self:Log("[Data Service] Operations queue is empty! Letting server shut down.")
	end)
end

----------------------------------------------------------------------------------------------------------------------------------------------------------------------
-- @Name : Stop
-- @Description : Called when the service is being stopped.
----------------------------------------------------------------------------------------------------------------------------------------------------------------------
function DataService:Stop()

	self:Log("[Data Service] Stopped!")
end

----------------------------------------------------------------------------------------------------------------------------------------------------------------------
-- @Name : Unload
-- @Description : Called when the service is being unloaded.
----------------------------------------------------------------------------------------------------------------------------------------------------------------------
function DataService:Unload()

	self:Log("[Data Service] Unloaded!")
end

return DataService