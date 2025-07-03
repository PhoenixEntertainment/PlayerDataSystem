local Players = game:GetService("Players")

while true do
	if shared.DragonEngine ~= nil then
		break
	else
		task.wait()
	end
end

local DataController = shared.DragonEngine:GetController("DataController")

local LocalPlayer = Players.LocalPlayer
local UI = Instance.new("ScreenGui")
local TextLabel = Instance.new("TextLabel")

DataController:OnDataChanged("Currency", function(Player, CurrencyType, NewGold, OldGold)
	if CurrencyType == "Gold" then
		print("GoldChange", Player, NewGold, OldGold)
		TextLabel.Text = "Gold: " .. tostring(DataController:ReadData("GetGold"))
	end
end)

TextLabel.Parent = UI
TextLabel.Text = "Gold: " .. tostring(DataController:ReadData("GetGold"))
TextLabel.Size = UDim2.fromScale(0.5, 0.1)
TextLabel.AnchorPoint = Vector2.new(0, 1)
TextLabel.Position = UDim2.fromScale(0, 1)
TextLabel.TextScaled = true
TextLabel.BackgroundTransparency = 1
TextLabel.TextColor3 = Color3.fromRGB(255, 255, 255)
UI.Parent = Players.LocalPlayer:WaitForChild("PlayerGui")
