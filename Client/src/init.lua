--[[
	Data controller
	Handles the fetching of the player's data
--]]

local DataController = {}

---------------------
-- Roblox Services --
---------------------
local ReplicatedStorage = game:GetService("ReplicatedStorage")
local Players = game:GetService("Players")
local RunService = game:GetService("RunService")

------------------
-- Dependencies --
------------------
local DataService;

-------------
-- Defines --
-------------
local DataCache;
local PlayerData;
local IsDataLoaded = false

----------------------------------------------------------------------------------------------------------------------------------------------------------------------------
-- @Name : IsDataLoaded
-- @Description : Returns a bool describing whether or not the player's data has been fully replicated in
----------------------------------------------------------------------------------------------------------------------------------------------------------------------------
function DataController:IsDataLoaded()
	return IsDataLoaded
end

----------------------------------------------------------------------------------------------------------------------------------------------------------------------------
-- @Name : GetData
-- @Description : Gets the player's data
-- @Params : bool "YieldForLoad" - A bool describing whether or not the API will yield for the data to exist
----------------------------------------------------------------------------------------------------------------------------------------------------------------------------
function DataController:GetData(YieldForLoad)
	if YieldForLoad then
		while true do
			if self:IsDataLoaded() then
				break
			else
				RunService.Stepped:wait()
			end
		end
	end

	return PlayerData
end

----------------------------------------------------------------------------------------------------------------------------------------------------------------------------
-- @Name : Init
-- @Description : Used to initialize controller state
----------------------------------------------------------------------------------------------------------------------------------------------------------------------------
function DataController:Init()
	self:DebugLog("[Data Controller] Initializing...")

	DataService = self:GetService("DataService")

	-----------------------------------
	-- Waiting for data to be loaded --
	-----------------------------------
	local Loaded = false
	local LoadedID = DataService:GetDataLoadedQueueID()

	DataService.DataLoaded:connect(function(QueueID)
		if QueueID == LoadedID then
			Loaded = true
		end
	end)

	while true do
		if Loaded then
			break
		else
			RunService.Stepped:wait()
		end
	end

	local DescendantCount = DataService:GetDataFolderDescendantCount()
	DataCache = ReplicatedStorage:WaitForChild("_DataCache")
	PlayerData = DataCache:WaitForChild(tostring(Players.LocalPlayer.UserId))

	while true do
		if #self:GetData():GetDescendants() >= DescendantCount then
			break
		end
		RunService.Stepped:wait()
	end
	IsDataLoaded = true

	self:DebugLog("[Data Controller] Initialized!")
end

----------------------------------------------------------------------------------------------------------------------------------------------------------------------------
-- @Name : Start
-- @Description : Used to run the controller
----------------------------------------------------------------------------------------------------------------------------------------------------------------------------
function DataController:Start()
	self:DebugLog("[Data Controller] Running!")
	
end

return DataController