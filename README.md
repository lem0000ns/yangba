## YangBA Data Fetcher (Backend)

This is the backend repository for the YangBA Data Fetcher, where all player/game data is retrieved from the NBA API. Python scripts were written to automate this process every 24 hours, ensuring its access and scope to the most recent information available. Player information from Wikipedia, which is later used to implement the player profile component in the frontend, is also webscraped on an automated process. This repository also handles route handling from AWS API Gateway, in which it reads a series of query string parameters and returns a result based on those parameters.

There are currently three different types of functionalities that the YangBA supports: Stats, Games, and Rank.

1. The "Stats" component retrives a single stat from an individual player, such as "Most 3PM Stephen Curry has made in 2017" or "What is the least amount of assists Lebron James had in a game where he scored more than 40 points?"
2. The "Games" component returns a series of statlines that fit within a certain criteria, given by the user. For example, it can show all statlines of players in which they've scored more than 50 points, or all games from a specific player in which they had more than 10 threes.
3. The "Rank" component ranks a series of players based on a certain criteria, given by the user. For example, if the user wanted to know the top 10 players based on free-throw efficiency or the worst 10 players in terms of points per game, they would choose this component.

Additionally, there is a custom bonus stat called "OPI" (Offensive Performance Index) that was created to place a single value for a particular player's statline. Factors that contribute to OPI include points, efficiency, assists, turnovers, minutes played, and total points scored by the team. OPI values were normalized such that all values fall between 0 to 1.

Frontend can be found here --> https://github.com/lem0000ns/yba_frontend

## Tech Stack

JSON, Requests, MySQL, Boto3, Beautiful Soup
AWS (Lambda, S3, API Gateway, Cloudfront, RDS, EC2)
