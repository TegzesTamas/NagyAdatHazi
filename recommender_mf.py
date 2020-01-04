#!/usr/bin/env python3

import pandas as pd

ratings = pd.read_csv("data/ratings.train", sep=" ", header=None, names=["UserId", "MovieId", "Rating", "Timestamp"])

# ratings = ratings.pivot_table(index=["MovieId", "UserId"], values="Rating")
