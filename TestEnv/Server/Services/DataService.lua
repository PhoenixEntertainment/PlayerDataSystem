local ReplicatedStorage = game:GetService("ReplicatedStorage")

local DataService = require(ReplicatedStorage.Packages["playerdatasystem-server"])

DataService:Configure({
	DatastoreName = "SysTest1",
	Schema = {
		Version = 1,
		Data = {
			OwnedItems = {
				Weapons = { "Red Crystal Sword" },
				Consumables = { "Health potion" },
			},
			Gold = 100,
			Level = 10,
			XP = 0,
		},
		Migrators = {},
	},
})

return DataService
