from database import *

engine = DatabaseEngine()

engine.create_database("NewTest")

db = DB(engine, "NewTest")

db.connect()

@database(db)
class User(Table):
    _id: int
    name: str
    tag: int

u1 = User(_id=1, name="HoldUp", tag=1783)
u2 = User(_id=2, name="HoldUp", tag=1683)

User.insert(u1)
User.insert(u2)

User.search(_id=1)


User.update({"_id":2}, {"name":"NotHoldUp"})

User.search(_id=2)

User.delete(_id=1)

db.disconnect()