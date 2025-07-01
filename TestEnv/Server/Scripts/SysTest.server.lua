local Players = game:GetService("Players")

while true do
	if shared.DragonEngine ~= nil then
		break
	else
		task.wait()
	end
end

local DataService = shared.DragonEngine:GetService("DataService")

local function PlayerAdded(Player)
	print("Reading data...")
	print(DataService:ReadData(Player, "GetOwnedWeapons"))

	while true do
		task.wait(1)
		DataService:WriteData(Player, "GiveGold", 1)
	end
end

for _, Player in pairs(Players:GetPlayers()) do
	task.spawn(PlayerAdded, Player)
end
Players.PlayerAdded:connect(PlayerAdded)

DataService:OnDataChanged("Currency", function(Player, CurrencyType, NewGold, OldGold)
	if CurrencyType == "Gold" then
		print("GoldChange", Player, NewGold, OldGold)
	end
end)
