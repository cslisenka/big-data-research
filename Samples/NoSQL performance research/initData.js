/* Init sample telecom data
 Sample data set contains of collection "calls"
*/

function getRandom4Digit() {
	return Math.floor(Math.random() * 10000)
}

function getRandom2Digit() {
	return Math.floor(Math.random() * 100)
}

function getRandom1Digit() {
	return Math.floor(Math.random() * 10)
}

var countries = ["Belarus", "Netherlands", "Poland", "Ukraine", "Litva", "Latvia", "Russia", "Germany", "Estonia", "Serbia"]
var providers = ["T-mobile", "Orange", "Vadafone", "Velcom", "Life", "Tele-2", "Rostelecom", "Beltelecom", "Jota", "O2"]

for (var i = 0; i < 100000000; i++) {
	db.calls.insert({
		phoneFrom : '+34500' + getRandom4Digit(),
		phoneTo : '+34500' + getRandom4Digit(),
		countryFrom : countries[getRandom1Digit()],
		countryTo : countries[getRandom1Digit()],
		cost : getRandom4Digit(),
		durationMinutes: getRandom2Digit(),
		sourceProvider : providers[getRandom1Digit()],
		targetProvider : providers[getRandom1Digit()]
	})
}
