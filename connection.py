import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT 

def set_up(cur):
	"""
	This method will compose the DB structure
	"""
	cur.execute("CREATE TABLE dataUser(email varchar(50) PRIMARY KEY,first_name varchar(50),last_name varchar(50), password varchar(30) NOT NULL);") 
	cur.execute("CREATE TABLE Seller(email varchar(50),lowest_price int,PRIMARY KEY (email),FOREIGN KEY (email) REFERENCES dataUser(email) ON DELETE CASCADE);")
	cur.execute("CREATE TABLE Buyer(email varchar(50),highest_price int, PRIMARY KEY (email), FOREIGN KEY (email) REFERENCES dataUser(email) ON DELETE CASCADE);")
	cur.execute("CREATE TABLE Brokerage(name varchar(50), address varchar(100), commission real, PRIMARY KEY (name,address));")
	cur.execute("CREATE TABLE Agent(email varchar(50), brName varchar(50) NOT NULL, brAddress varchar(100) NOT NULL, PRIMARY KEY (email), FOREIGN KEY (email) REFERENCES dataUser(email) ON DELETE CASCADE,FOREIGN KEY (brName,brAddress) REFERENCES Brokerage(name,address));")
	cur.execute("CREATE TABLE House(address varchar(100), unitNum int, price bigint, PRIMARY KEY (address,unitNum));")
	cur.execute("CREATE TABLE Deal(id serial, address varchar(100) NOT NULL, unitNum int NOT NULL, status varchar(50), sellAgent varchar(50) NOT NULL, seller1 varchar(50), seller2 varchar(50),buyAgent varchar(50), buyer1 varchar(50), buyer2 varchar(50), PRIMARY KEY (id), FOREIGN KEY (address,unitNum) REFERENCES House(address,unitNum),FOREIGN KEY (buyer1) REFERENCES Buyer(email) ON DELETE SET NULL, FOREIGN KEY (buyer2) REFERENCES Buyer(email) ON DELETE SET NULL, FOREIGN KEY (seller1) REFERENCES Seller(email) ON DELETE SET NULL, FOREIGN KEY (seller2) REFERENCES Seller(email) ON DELETE SET NULL, FOREIGN KEY (sellAgent) REFERENCES Agent(email) ON DELETE CASCADE, FOREIGN KEY (buyAgent) REFERENCES Agent(email) ON DELETE SET NULL);")
	cur.execute("CREATE TABLE Document(name varchar(50),id int,version int,fileDoc varchar(10000),is_signed boolean, PRIMARY KEY (name,id,version),FOREIGN KEY (id) REFERENCES Deal(id) ON DELETE CASCADE);")
	cur.execute("CREATE TABLE Message(dateAndTime varchar(50),text varchar(1000),email varchar(50),id int NOT NULL,PRIMARY KEY (email, dateAndTime), FOREIGN KEY (email) REFERENCES dataUser(email) ON DELETE CASCADE, FOREIGN KEY (id) REFERENCES Deal(id) ON DELETE CASCADE);")


def populate(cur):
	"""
	This method will put sample data into the DB
	"""
	# Data Users
	cur.execute("INSERT INTO dataUser VALUES('user1@gmail.com','Alice','Smith','pass1234');")
	cur.execute("INSERT INTO dataUser VALUES('user2@gmail.com','Bob','Smith','dogo2020');")
	cur.execute("INSERT INTO dataUser VALUES('user3@gmail.com','Kevin','Smith','snowboard_time');")
	cur.execute("INSERT INTO dataUser VALUES('user4@gmail.com','Calvin','Johns','il0v3c4t5');")
	cur.execute("INSERT INTO dataUser VALUES('user5@gmail.com','Kevin','Benz','12345678890');")
	cur.execute("INSERT INTO dataUser VALUES('user6@gmail.com','Amy','Appleseed','6135552020');")
	cur.execute("INSERT INTO dataUser VALUES('user7@gmail.com','Mister','Manley','manley1414');")
	cur.execute("INSERT INTO dataUser VALUES('user8@gmail.com','Misses','Manley','miss_man1');")
	cur.execute("INSERT INTO dataUser VALUES('user9@gmail.com','Betty','Buyer','buydatdank');")
	cur.execute("INSERT INTO dataUser VALUES('user10@gmail.com','Brendan','Buyer','buydatdank420');")
	cur.execute("INSERT INTO Brokerage VALUES('Little Brokers','123 Berry Ave', 2.5);")
	cur.execute("INSERT INTO Brokerage VALUES('Buy a Home Inc.','456 Apple Ave', 1);")
	cur.execute("INSERT INTO Brokerage VALUES('Green-Key','5050 Commission St.', 5);")
	cur.execute("INSERT INTO Brokerage VALUES('Comfree','0 Commission St.', 0);")
	cur.execute("INSERT INTO Brokerage VALUES('Luxury Service Brokers','999 Pro Road', 9);")
	cur.execute("INSERT INTO Brokerage VALUES('Bad Brokers','10 Bad Place', 20.5);")
	# Agent Users
	cur.execute("INSERT INTO Agent VALUES('user1@gmail.com', 'Little Brokers', '123 Berry Ave');")
	cur.execute("INSERT INTO Agent VALUES('user2@gmail.com', 'Buy a Home Inc.', '456 Apple Ave');")
	cur.execute("INSERT INTO Agent VALUES('user5@gmail.com', 'Green-Key', '5050 Commission St.');")
	cur.execute("INSERT INTO Agent VALUES('user6@gmail.com', 'Comfree', '0 Commission St.');")
	cur.execute("INSERT INTO Agent VALUES('user4@gmail.com', 'Luxury Service Brokers', '999 Pro Road');")
	cur.execute("INSERT INTO Agent VALUES('user3@gmail.com', 'Bad Brokers', '10 Bad Place');")
	# Seller Users
	cur.execute("INSERT INTO Seller VALUES('user7@gmail.com', 800000);")
	cur.execute("INSERT INTO Seller VALUES('user8@gmail.com', 850000);")
	cur.execute("INSERT INTO Seller VALUES('user1@gmail.com', 50000);")
	cur.execute("INSERT INTO Seller VALUES('user2@gmail.com', 1000000);")
	cur.execute("INSERT INTO Seller VALUES('user3@gmail.com', 1250000);")
	cur.execute("INSERT INTO Seller VALUES('user4@gmail.com', 1250000);")
	# Buyer Users
	cur.execute("INSERT INTO Buyer VALUES('user4@gmail.com', 100000);")
	cur.execute("INSERT INTO Buyer VALUES('user5@gmail.com', 400000);")
	cur.execute("INSERT INTO Buyer VALUES('user6@gmail.com', 800000);")
	cur.execute("INSERT INTO Buyer VALUES('user9@gmail.com', 1200000);")
	cur.execute("INSERT INTO Buyer VALUES('user10@gmail.com', 1600000);")
	# Houses
	cur.execute("INSERT INTO House VALUES('20 Hally Ave', 2, 500000);")
	cur.execute("INSERT INTO House VALUES('25 Gray Street', 0, 800000);")
	cur.execute("INSERT INTO House VALUES('253 Maripose Ave', 3, 850000);")
	cur.execute("INSERT INTO House VALUES('202 Rue Universirt', 14, 825000);")
	cur.execute("INSERT INTO House VALUES('1023 King St W.', 1408, 1000000);")
	cur.execute("INSERT INTO House VALUES('1023 King St W.', 1409, 1500000);")
	cur.execute("INSERT INTO House VALUES('909 Nike Street', 0, 20000000);")
	cur.execute("INSERT INTO House VALUES('3209 Balmer Road', 1408, 100000);")
	cur.execute("INSERT INTO House VALUES('84 Monster Place', 0, 500);")
	# Deals
	cur.execute("INSERT INTO Deal VALUES(1, '20 Hally Ave', 2, 'Listed', 'user5@gmail.com', 'user1@gmail.com', null, null, null, null);")
	cur.execute("INSERT INTO Deal VALUES(2, '25 Gray Street', 0, 'Getting Listed', 'user5@gmail.com', 'user1@gmail.com', 'user2@gmail.com', null, null, null);")
	cur.execute("INSERT INTO Deal VALUES(3, '253 Maripose Ave', 3, 'Closing', 'user1@gmail.com', 'user7@gmail.com', 'user8@gmail.com', 'user1@gmail.com', 'user9@gmail.com', 'user10@gmail.com');")
	cur.execute("INSERT INTO Deal VALUES(4, '1023 King St W.', 1408, 'Negotiating', 'user4@gmail.com', 'user4@gmail.com', null, 'user2@gmail.com', 'user5@gmail.com', 'user6@gmail.com');")
	cur.execute("INSERT INTO Deal VALUES(1000, '1023 King St W.', 1409, 'Negotiating', 	'user4@gmail.com', 'user4@gmail.com', 'user3@gmail.com', 'user1@gmail.com', 'user9@gmail.com', 'user10@gmail.com');")
	cur.execute("INSERT INTO Deal VALUES(1010, '84 Monster Place', 0, 'Listed','user6@gmail.com', null, null, null, null, null);")
	cur.execute("INSERT INTO Deal VALUES(1011, '909 Nike Street', 0, 'Closing','user6@gmail.com', null, null, 'user1@gmail.com', null, null);")
	cur.execute("INSERT INTO Deal VALUES(1012, '3209 Balmer Road', 1408, 'Closing','user6@gmail.com', 'user4@gmail.com', null, null, 'user10@gmail.com', null);")
	cur.execute("INSERT INTO Deal VALUES(2000, '202 Rue Universirt', 14, 'Listed','user6@gmail.com', null, null, null, 'user4@gmail.com', null);")
	# Documents
	cur.execute("INSERT INTO Document VALUES('Listing Agreement', 1, 1, 'This document lets me sell your house!', '0');")
	cur.execute("INSERT INTO Document VALUES('Listing Agreement', 1, 2, 'This document lets me sell your house!', '1');")
	cur.execute("INSERT INTO Document VALUES('Working With a REALTOR', 1, 1, 'You are working with an official REALTOR', '0');")
	cur.execute("INSERT INTO Document VALUES('Working With a REALTOR', 1, 2, 'You are working with an official REALTOR', '1');")
	cur.execute("INSERT INTO Document VALUES('Selling Plan', 1, 1000, 'We are going to sell to developers and here is why...', '0');")
	cur.execute("INSERT INTO Document VALUES('Selling Plan Ppt', 1, 1010, 'Here are some images', '0');")
	cur.execute("INSERT INTO Document VALUES('Notice of Condition Fufillment', 1, 1, 'The house is hereby inspecrted', 'true');")
	# Messages
	cur.execute("INSERT INTO Message VALUES('20120618 10:34:09 AM', 'Is this home online yet', 'user1@gmail.com', 1);")
	cur.execute("INSERT INTO Message VALUES('20120618 10:34:09 AM', 'Is this home up online yet', 'user2@gmail.com', 2);")
	cur.execute("INSERT INTO Message VALUES('20120619 10:34:59 AM', 'Is this home for sale?', 'user10@gmail.com', 3);")
	cur.execute("INSERT INTO Message VALUES('20120619 10:35:29 AM', 'Yeah, you can buy it for $850000', 'user1@gmail.com', 3);")
	cur.execute("INSERT INTO Message VALUES('20120619 10:36:20 AM', 'I''ll go $825k and thats final.', 'user9@gmail.com', 3);")
	cur.execute("INSERT INTO Message VALUES('20120619 10:36:25 AM', 'SOLD!', 'user7@gmail.com', 3);")
	cur.execute("INSERT INTO Message VALUES('20190619 10:36:25 AM', 'It''s 2019 and still no offers...', 'user6@gmail.com', 1010);")
	cur.execute("INSERT INTO Message VALUES('20190619 10:36:26 AM', 'Eff me...', 'user6@gmail.com', 1010);")

def tear_down(cur):
	"""
	This method will tear down the DB
	"""
	cur.execute("DROP TABLE dataUser CASCADE;")
	cur.execute("DROP TABLE Seller CASCADE;")
	cur.execute("DROP TABLE Buyer CASCADE;")
	cur.execute("DROP TABLE Brokerage CASCADE;")
	cur.execute("DROP TABLE Agent CASCADE;")
	cur.execute("DROP TABLE House CASCADE;")
	cur.execute("DROP TABLE Deal CASCADE;")
	cur.execute("DROP TABLE Document CASCADE;")
	cur.execute("DROP TABLE Message CASCADE;")
	

if __name__ == '__main__':

	# Print start statement
	print('Slack for Real Estate!')

	# Connect to DB
	user = str(raw_input("Enter your postgres user: "))

	# Connect per postgres user with default port
	con = psycopg2.connect(
		dbname='postgres',
		user=user, 
		password='pass1234',
		host=''
	)

	# Set connection and cursor
	con.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
	cur = con.cursor()

	# Set up the initial database
	set_up(cur)
	populate(cur)

	running = True
	while running:
		try:
			# Try to get a number
			print('Enter one of the following commands:')
			print('1 - Find the most successful agents (closing the most deals)')
			print('2 - Delete the #1 most successful agent')
			print('3 - Insert a new email for a user')
			print('4 - Remove improper emails (check for @ char)')
			print('5 - Quit')
			cmd = int(input("Enter: "))

			# Validtae the input
			if cmd <= 0 or 5 < cmd:
				raise Exception('Improper input')

			# Make space
			print('')

			# Parse the input for commands
			if cmd == 1:
				"""
				This command will find the First and Last name of the Seller Agents who are closing the most deals
				"""
				print('Fetching...')
				cur.execute("SELECT u.first_name as First, u.last_name as Last FROM dataUser u INNER JOIN Agent a ON u.email = a.email WHERE a.email IN (SELECT sellAgent as num FROM Deal WHERE status = 'Closing' GROUP BY sellAgent ORDER BY num DESC);")
				rows = cur.fetchall()
				if len(rows) is 0:
					print("None left :(")
				for i in range(0, len(rows)):
					row = rows[i]
					print(str(i+1) + ": " + row[0] + " " + row[1])

			if cmd == 2:
				"""
				This method will delete the agent closing the most deals
				"""
				print('Deleting...')
				cur.execute("DELETE FROM dataUser WHERE dataUser.email IN (SELECT u.email as email FROM dataUser u INNER JOIN Agent a ON u.email = a.email WHERE a.email IN (SELECT sellAgent as num FROM Deal WHERE status = 'Closing' GROUP BY sellAgent ORDER BY num DESC LIMIT 1));")
				print('Done')

			if cmd == 3:
				print('Fetching...')

				# Get the current email
				old_email = str(raw_input("Current email: "))

				# Get the user
				cur.execute("SELECT * FROM dataUser WHERE email=\'" + old_email + "';")
				rows = cur.fetchall()
				if len(rows) is 0:
					print('Nobody has email: ' + old_email)
					print('')
					continue
				first_name = rows[0][1]
				last_name = rows[0][2]
				password = rows[0][3]
				
				# Ask for new email
				new_email = str(raw_input("New email: "))
				
				# Delete the user
				cur.execute("DELETE FROM dataUser WHERE email=\'" + old_email + "';")
				
				# Add as new user
				cur.execute("INSERT INTO dataUser VALUES(\'" + new_email + "',\'" + first_name + "',\'" + last_name + "',\'" + password + "');")

			if cmd == 4:
				print('Fetching...')
				cur.execute("SELECT * FROM dataUser;")
				rows = cur.fetchall()
				for row in rows:
					if '@' not in row[0]:
						print("Deleting user with email: " + row[0])
						cur.execute("DELETE FROM dataUser WHERE email=\'" + row[0] + "';")
				print('Done')

			if cmd == 5:
				"""
				Exit the loop
				"""
				print('Quitting...')
				running = False

			# Leave space for the next command(s)
			print('')

		except:
			print('ERR: Input must be 1, 2, 3, 4, or 5.')
			print('')

	tear_down(cur)
	cur.close()
	con.close()