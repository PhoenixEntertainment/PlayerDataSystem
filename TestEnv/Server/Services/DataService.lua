local ReplicatedStorage = game:GetService("ReplicatedStorage")

local DataService = require(ReplicatedStorage.Packages["playerdatasystem-server"])

DataService:SetConfigs({
	DataFormatVersion = 2,
	DataFormat = {
		Coins = 0,
		XP = 0,
		Color = "Purple"
	},
	DataFormatConversions = {
		["1 -> 2"] = function(Data)
			Data.Color = "Purple"

			return Data
		end
	},
	DatastoreBaseName = "Test",
	DatastorePreciseName = "PlayerData1",
	DatastoreRetryEnabled = true,
	DatastoreRetryInterval = 3,
	DatastoreRetryLimit = 2,
	SessionLockYieldInterval = 5,
	SessionLockMaxYieldIntervals = 5,
	DataKeyName = "SaveData"
})

return DataService