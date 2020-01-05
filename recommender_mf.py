#!/usr/bin/env python3

import numpy as np
import pandas as pd
import multiprocessing as mp
import threading


def iterate(pair):
    print(pair)


def learn():
    index, row = pair
    user_id = row["UserId"] - 1
    movie_id = row["MovieId"] - 1
    rating = row["Rating"]
    this_user_feat = old_user_features[user_id, :]
    new_this_user_feat = new_user_features[user_id, :]
    this_movie_feat = old_movie_features[movie_id, :]
    new_this_movie_feat = new_movie_features[movie_id, :]
    guess = np.dot(this_user_feat, this_movie_feat.T)
    curr_error = (guess - rating)
    for feature_id in range(num_of_features):
        change = 2 * curr_error * this_movie_feat[feature_id] * learning_rate
        users_lock.acquire()
        new_this_user_feat[feature_id] += change
        users_lock.release()

        change = 2 * curr_error * this_user_feat[feature_id] * learning_rate
        movies_lock.acquire()
        new_this_movie_feat[feature_id] += change
        movies_lock.release()
    return curr_error * curr_error


if __name__ == '__main__':
    ratings = pd.read_csv("data/ratings.train", sep=" ", header=None,
                          names=["UserId", "MovieId", "Rating", "Timestamp"])
    ratings = ratings[:1000]
    # ratings = ratings.pivot_table(index=["UserId"],columns="MovieId",values="Rating")

    num_of_movies = ratings["MovieId"].max()
    num_of_users = ratings["UserId"].max()

    num_of_features = 100

    # Goal: ratings = user_features * movie_features.T

    new_user_features = np.random.rand(num_of_users, num_of_features)
    new_movie_features = np.random.rand(num_of_movies, num_of_features)

    steps = 1000

    learning_rate = .0002

    p = mp.Pool()
    for i in range(steps):
        print()
        print()
        print(f"STEP {i}")
        print()
        print()
        sum_sq_error = 0
        # old_user_features = new_user_features
        # old_movie_features = new_movie_features
        # new_user_features = old_user_features.copy()
        # new_movie_features = old_movie_features.copy()
        # users_lock = threading.Lock()
        # movies_lock = threading.Lock()
        p.map(print, ratings.iterrows())
        print(f"Sum Squared Error = {sum_sq_error}")
