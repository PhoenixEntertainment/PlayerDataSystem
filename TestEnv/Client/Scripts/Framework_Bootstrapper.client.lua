local ReplicatedStorage = game:GetService("ReplicatedStorage")
local Players = game:GetService("Players")

local DragonEngine = require(ReplicatedStorage.Packages.DragonEngine)

DragonEngine:Run({
	ShowLogoInOutput = false,
	Debug = true,
	ClientPaths = {
		ModulePaths = {
			Shared = {
				ReplicatedStorage.Packages
			},
			Client = {
			}
		},
	
		ControllerPaths = {
			Players.LocalPlayer.PlayerScripts:WaitForChild("Controllers")
		}
	}
})