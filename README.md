# Movie-Recommendation-System
This is a Movie Recommendation System using Item-based Collaborative Filtering with Hadoop MapReduce.

Data comes from the training dataset of Netflix Prize Challenge.

Multiple MapReduce jobs are used to build and normalize Co-occurrence Matrix, multiply Co-occurrence matrix with rating matrix, and generate top k movies that are most likely to be watched by a user.

Data Preprocessing:
      Modify the original dataset to the form: UserId, MovieId, rating.
Individual Job:
      DivideDataByUserId: read in each line of the dataset, output a file has the following format - key: UserId, value: movieId1:rating1, movieId2: rating2,...
