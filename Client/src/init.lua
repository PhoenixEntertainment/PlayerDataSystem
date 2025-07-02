local DataController = {}

---------------------
-- Roblox Services --
---------------------
local Players = game:GetService("Players")

------------------
-- Dependencies --
------------------
local DataService
local DataHandlers
local Table = require(script.Parent.Table)

-------------
-- Defines --
-------------
local DATA_READERS = {}
local DATA_WRITERS = {}
local EVENTS = {}
local DataCache
local CurrentDataSessionID = ""
local LocalPlayer = Players.LocalPlayer

----------------------------------------------------------------------------------------------------------------------------------------------------------------------
-- Helper functions
----------------------------------------------------------------------------------------------------------------------------------------------------------------------
local function WriteData(Writer, ...)
	DATA_WRITERS[Writer](DataCache, ...)
end

----------------------------------------------------------------------------------------------------------------------------------------------------------------------
-- API Methods
----------------------------------------------------------------------------------------------------------------------------------------------------------------------

----------------------------------------------------------------------------------------------------------------------------------------------------------------------
-- @Name : ReadData
-- @Description : Calls the specified reader function which reads the given player's savedata
-- @Params : string "Reader" - The name of the reader function to call
--           Tuple "Args" - The arguments to pass to the specified reader function
----------------------------------------------------------------------------------------------------------------------------------------------------------------------
function DataController:ReadData(Reader, ...)
	while true do
		if not LocalPlayer:IsDescendantOf(game) then
			return nil
		elseif DataCache ~= nil then
			break
		end

		task.wait()
	end

	return DATA_READERS[Reader](Table.Copy(DataCache, true), ...)
end

----------------------------------------------------------------------------------------------------------------------------------------------------------------------
-- @Name : Init
-- @Description : Called when the service module is first loaded.
----------------------------------------------------------------------------------------------------------------------------------------------------------------------
function DataController:Init()
	self:DebugLog("[Data Controller] Initializing...")

	DataService = self:GetService("DataService")
	DataHandlers = require(DataService:GetDataHandlerModule())
	DATA_WRITERS = DataHandlers.Writers
	DATA_READERS = DataHandlers.Readers

	self:DebugLog("[Data Controller] Initialized!")
end

----------------------------------------------------------------------------------------------------------------------------------------------------------------------
-- @Name : Start
-- @Description : Called after all services are loaded.
----------------------------------------------------------------------------------------------------------------------------------------------------------------------
function DataController:Start()
	self:DebugLog("[Data Controller] Running!")

	--------------------------------
	-- Getting current session ID --
	--------------------------------
	while true do
		local DataSessionID = LocalPlayer:GetAttribute("SaveSessionID")

		if DataSessionID ~= nil then
			CurrentDataSessionID = DataSessionID

			break
		end
		task.wait(0.5)
	end

	DataService.DataLoaded:connect(function(SessionID)
		if SessionID == CurrentDataSessionID then
			DataCache = DataService:RequestRawData()

			print("[Data]", DataCache)
		end
	end)

	DataService.DataWritten:connect(function(Writer, ...)
		if DataCache ~= nil then
			WriteData(Writer, ...)
		end
		print("[New Data]", DataCache)
	end)
end

return DataController
