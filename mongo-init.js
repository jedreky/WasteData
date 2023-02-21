db = db.getSiblingDB("WasteData")
db.createUser({user: "user", pwd: "user_password", roles: [ { role: "readWrite", db: "WasteData" }] })
