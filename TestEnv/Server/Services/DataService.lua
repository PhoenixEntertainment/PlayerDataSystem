local ReplicatedStorage = game:GetService("ReplicatedStorage")

local DataService = require(ReplicatedStorage.Packages["playerdatasystem-server"])

DataService:Configure({
	DataHandlers = ReplicatedStorage.DataHandlers,
	DatastoreName = "SysTest1",
	Schema = {
		Version = 2,
		Data = {
			OwnedItems = {
				Weapons = { "Red Crystal Sword" },
				Consumables = { "Health potion" },
			},
			Currency = {
				Gold = 100,
				Gems = 0,
			},
			Level = 10,
			XP = 0,
		},
		Migrators = {
			["1 -> 2"] = function(Data)
				Data.Currency = {
					Gold = Data.Gold,
					Gems = 0,
				}
				Data.Gold = nil

				return Data
			end,
		},
	},
})

return DataService
