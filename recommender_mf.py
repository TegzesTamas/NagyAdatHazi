#!/usr/bin/env python3

import numpy as np
import pandas as pd

ratings = pd.read_csv("data/ratings.train", sep=" ", header=None, names=["UserId", "MovieId", "Rating", "Timestamp"])
# ratings = ratings.pivot_table(index=["UserId"],columns="MovieId",values="Rating")

num_of_movies = ratings["MovieId"].max()
num_of_users = ratings["UserId"].max()

num_of_features = 100

# Goal: ratings = user_features * movie_features.T

user_features = np.random.rand(num_of_users, num_of_features)
movie_features = np.random.rand(num_of_movies, num_of_features)

steps = 100000

for i in range(steps):
    print(f"STEP {i}")
    for index, row in ratings.iterrows():
        user_id = row["UserId"] - 1
        movie_id = row["MovieId"] - 1
        rating = row["Rating"]
        guess = np.dot(user_features[user_id, :],
                       movie_features[movie_id, :].T)
        print(f"{user_id}\t{movie_id}\t{rating}\t{guess}")
