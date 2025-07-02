return {
	Writers = {
		GiveGold = function(Data, GoldToAdd)
			local OldGold = Data.Currency.Gold

			Data.Currency.Gold = Data.Currency.Gold + GoldToAdd

			return "Currency", "Gold", Data.Currency.Gold, OldGold
		end,
	},
	Readers = {
		GetOwnedWeapons = function(Data)
			return Data.OwnedItems.Weapons
		end,

		GetGold = function(Data)
			return Data.Currency.Gold
		end,
	},
}
