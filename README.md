# Movie-Recommendation-System
This is a Movie Recommendation System using Item-based Collaborative Filtering with Hadoop MapReduce.

Data comes from the training dataset of Netflix Prize Challenge.

Multiple MapReduce jobs are used to build and normalize Co-occurrence Matrix, multiply Co-occurrence matrix with rating matrix, and generate top k movies that are most likely to be watched by a user.

Data Preprocessing:
      Modify the original dataset to the form: UserId, MovieId, rating.

Steps: 
Build Co-occurrence matrix,
Compute average score of each user,
Normalize Co-occurrence matrix,
Build rating matrix,
Multiply Co-occurrence matrix with rating matrix, when the rating doesn't exist, use the user's average score instead,
Generate recommendation list
