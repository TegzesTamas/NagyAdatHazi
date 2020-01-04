#!/usr/bin/env python3

import numpy as np
import pandas as pd

ratings = pd.read_csv("data/ratings.train", sep=" ", header=None, names=["UserId", "MovieId", "Rating", "Timestamp"])
ratings = ratings[:1000]
# ratings = ratings.pivot_table(index=["UserId"],columns="MovieId",values="Rating")

num_of_movies = ratings["MovieId"].max()
num_of_users = ratings["UserId"].max()

num_of_features = 100

# Goal: ratings = user_features * movie_features.T

user_features = np.random.rand(num_of_users, num_of_features)
movie_features = np.random.rand(num_of_movies, num_of_features)

steps = 1000

learning_rate = .0002

for i in range(steps):
    print(f"STEP {i}")
    sum_sq_error = 0
    for index, row in ratings.iterrows():
        user_id = row["UserId"] - 1
        movie_id = row["MovieId"] - 1
        rating = row["Rating"]
        this_user_feat = user_features[user_id, :]
        this_movie_feat = movie_features[movie_id, :]
        guess = np.dot(this_user_feat, this_movie_feat.T)
        curr_error = (guess - rating)
        sum_sq_error += curr_error * curr_error
        for feature_id in range(num_of_features):
            this_user_feat[feature_id] += 2 * curr_error * this_movie_feat[feature_id] * learning_rate
            this_movie_feat[feature_id] += 2 * curr_error * this_user_feat[feature_id] * learning_rate
        # print(f"{user_id}\t{movie_id}\t{rating}\t{guess}")
    print(f"Sum Squared Error = {sum_sq_error}")
