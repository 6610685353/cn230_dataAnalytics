# Siranat Phimphicharn 6610685353

import requests
import sqlite3
import datetime


url = "https://www.cheapshark.com/api/1.0/deals"
deals = requests.get(url).json()

con = sqlite3.connect("cheapshark.db")

cur = con.cursor()

cur.execute("DROP TABLE IF EXISTS deals")

cur.execute('''
CREATE TABLE deals (
    dealID TEXT PRIMARY KEY,
    storeID TEXT,
    gameID TEXT,
    name TEXT,
    salePrice REAL,
    normalPrice REAL,
    isOnSale INTEGER,
    savings REAL,
    metacriticScore INTEGER,
    steamRatingPercent INTEGER,
    steamRatingCount INTEGER,
    steamAppID TEXT,
    releaseDate INTEGER,
    lastChange INTEGER,
    dealRating REAL
)
''')

for d in deals:
    cur.execute('''
    INSERT OR REPLACE INTO deals VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ''', (
        d["dealID"], d["storeID"], d["gameID"], d["title"],
        float(d["salePrice"]), float(d["normalPrice"]),
        int(d["isOnSale"]), float(d["savings"]),
        int(d["metacriticScore"]) if d["metacriticScore"] else None,
        int(d["steamRatingPercent"]) if d["steamRatingPercent"] else None,
        int(d["steamRatingCount"]) if d["steamRatingCount"] else None,
        d["steamAppID"], d["releaseDate"], d["lastChange"],
        float(d["dealRating"])
    ))

con.commit()


print()
print("Game Deals Analytics from CheapShark API:")
print("------------------------------------------")

cur.execute("""
SELECT distinct name, normalPrice 
FROM deals 
WHERE normalPrice = (SELECT MAX(normalPrice) FROM deals)
""")
most_expensive_games = cur.fetchall()

print("Most expensive game(s):")
for game in most_expensive_games:
    print(f"- {game[0]} (${game[1]})")

print()

cur.execute("""
SELECT distinct name, normalPrice 
FROM deals x
WHERE normalPrice = (SELECT MIN(normalPrice) FROM deals)
""")
most_affordable_games = cur.fetchall()

print("Most affordable game(s):")
for game in most_affordable_games:
    print(f"- {game[0]} (${game[1]})")

print()

cur.execute("""
SELECT name, steamRatingPercent, metacriticScore, normalPrice
FROM deals
ORDER BY steamRatingPercent DESC
LIMIT 10
""")
print("Top 10 games based on Steam rating:")
i = 1
for info in cur.fetchall():
    print(f" {i}. {info[0]} ")
    i += 1
print()

cur.execute("""
SELECT distinct name, (100-(salePrice * 100)/ normalPrice) as saleRate, normalPrice, salePrice
FROM deals
ORDER BY saleRate DESC
LIMIT 5
""")

print("Top 5 game with the most discount rate: ")
i = 1
for info in cur.fetchall():
    print(f" {i}. Sale {round(info[1], 2)}%  {info[0]}")
    i += 1
print()


cur.execute("""
SELECT AVG(dealRating)
FROM deals
WHERE salePrice < 50
""")
print("Average dealRating where salePrice belows $50 dollars :", end = " ")
print(round(cur.fetchone()[0], 2))
print()

cur.execute("""SELECT storeID, COUNT(*) AS numDeals
FROM deals
GROUP BY storeID
having isOnSale = 1
ORDER BY numDeals DESC;
""")

print("Number of deals in each store where game is on sale: ")
for info in cur.fetchall():
    print(f" No. of deals = {info[1]} \t Store {info[0]} ")
print()

cur.execute(
"""SELECT storeID, AVG(salePrice) AS avgSalePrice
FROM deals
GROUP BY storeID
ORDER BY avgSalePrice DESC;
""")

print("Average sale price by store:")
i = 1
for info in cur.fetchall():
    print(f" Avg. sale price = ${round(info[1],2)}  \t Store {info[0]} ")
    i += 1
print()

cur.execute("""SELECT 
    distinct name,
    (steamRatingPercent - metacriticScore) AS difference
FROM deals
WHERE steamRatingPercent IS NOT NULL AND metacriticScore IS NOT NULL
ORDER BY difference DESC
LIMIT 5;"""
)

print("Top 5 game with the most critic difference between user and critic score: ")
i = 1
for info in cur.fetchall():
    print(f" {i}. Diff: {round(info[1],2)} \t {info[0]} ")
    i += 1
print()


cur.execute("""SELECT name, salePrice, dealRating
               FROM deals
               WHERE salePrice <= 5 AND dealRating > 9
               ORDER BY salePrice DESC
               LIMIT 5;""")
print("Top 5 games with deal rating more than 9 but sale price below 5 dollars")
i = 1
for info in cur.fetchall():
    print(f" {i}. {info[0]} (${round(info[1],2)})")
    i += 1
print()

cur.execute("""SELECT name, releaseDate
FROM deals
WHERE releaseDate IS NOT NULL
ORDER BY releaseDate DESC
LIMIT 1;
"""
)

print("Latest released game in store:")
for info in cur.fetchall():
    game_name = info[0] 
    timestamp = info[1]  
    release_date = datetime.datetime.fromtimestamp(int(timestamp), datetime.timezone.utc)
    formatted_date = release_date.strftime('%Y-%m-%d')
    print(f"- {game_name} \t release date: {formatted_date}")
print()


cur.execute("""
SELECT name, releaseDate
FROM deals
ORDER BY releaseDate ASC
LIMIT 1
""")
oldest_game = cur.fetchone()
game_name = oldest_game[0]
timestamp = oldest_game[1]
release_date = datetime.datetime.fromtimestamp(int(timestamp), datetime.timezone.utc)
formatted_date = release_date.strftime('%Y-%m-%d')
print("The game that has been released the longest: ")
print(f"- {game_name} \t release date: {formatted_date}")
print()

cur.execute("""
SELECT name, steamRatingCount
FROM deals
WHERE steamRatingCount IS NOT NULL
ORDER BY steamRatingCount DESC
LIMIT 10;
""")

top_10_games = cur.fetchall()

print("Top 10 games based on Steam review count:")
for idx, game in enumerate(top_10_games, start=1):
    print(f" {idx}. {game[0]} - {game[1]} reviews")
print()


cur.execute("""
SELECT storeID, 
       AVG(metacriticScore) AS avgMeta, 
       AVG(steamRatingPercent) AS avgSteam
FROM deals
WHERE metacriticScore IS NOT NULL AND steamRatingPercent IS NOT NULL
GROUP BY storeID
ORDER BY avgSteam DESC;
""")
avg_scores = cur.fetchall()
print("Average Metacritic and Steam rating by store:")
for i, row in enumerate(avg_scores, start=1):
    store, avg_meta, avg_steam = row
    print(f" {i}. Store {store} - Avg Metacritic: {round(avg_meta, 2)} | Avg Steam: {round(avg_steam, 2)}%")
print()

cur.execute("""
SELECT name, steamRatingPercent
FROM deals
WHERE steamRatingPercent IS NOT NULL
ORDER BY steamRatingPercent ASC
LIMIT 5;
""")

lowest_rated = cur.fetchall()

print("Top 5 games based on least Steam rating:")
for i, game in enumerate(lowest_rated, start=1):
    print(f" {i}. {game[0]} - Steam Rating: {game[1]}%")
print()

con.close()