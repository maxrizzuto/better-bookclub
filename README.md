# Overview
Hello! My name is Max. This is a personal project I'm working on. Goodreads and StoryGraph recommend books to you based on your reading habits, and also have functionality for book clubs--groups of people who are interested in reading a book together. However, neither has functionality (to the best of my knowledge) to **recommend books for those book clubs** instead of individuals. I'd like to make a website/app that introduces this functionality. Export yours and your friends' Goodreads and StoryGraph data (hopefully you'll be able to just add profile URLs in the future) and this website will recommend books for you to read as a group!

I'll have some documentation below that outlines my decision-making processes, how the app will work, and more details. Feel free to reach out w/ any questions or feature requests :)

# Journal Documentation
## Week 1 Decisions

### Dataset:

- [Amazon Books and Kindle Store](https://www.kaggle.com/datasets/khoa3chuwxa/amazon-books-and-kindle-store) (2025)
    - Has product metadata and reviews for a ton of books from Amazon and Kindle, 29GB total
    - [Amazon Book Reviews Description Embeddings](https://www.kaggle.com/datasets/lizettelemus/amazon-book-reviews-description-embeddings) dataset has embeddings for Amazon books--could be used for comparisons w/ books w/ unseen reviews
- [Open Library API](https://openlibrary.org/developers/api)
    - For getting book covers, maybe metadata later, info for new books? Primarily book covers. Match on ASIN (ISBN)

### Machine Learning

#### Training

1. Impute **missing ratings** using Neural Collaborative Filtering. Use content-based data for input to address potential cold start problem
    1. ReLU activation functions for hidden layers
    2. Tower pattern layers (halve number of neurons between each layer)
    3. Use book data for item input (e.g. genre, metadata, description embedding)
2. Use a **preference aggregation** strategy to make predictions for a group (Copeland score?)

#### Testing

1. Generate **synthetic groups** using user-user similarity scores, groups of people who all clearly have similar preferences
2. Evaluate recommendations manually


# Future Enhancements

## Test predictions of decisions w/ synthetic group profiles and decisions

1. Impute **missing ratings** 
2. Create **synthetic groups** (10fc), grouping people by user similarity scores (user-user collaborative filtering). Introduce some stochasticity in there too for a breadth of group preferences
3. Create **group profiles** using an aggregation method tbd (likely Copeland score)
4. Create **synthetic group decisions** using a variety of aggregation methods (e.g. some averages, some 	copeland scores, some borda counts, etc.)
5. Conduct **Neural Collaborative Filtering** to predict group's ratings?

## Add user surveys to address cold-start problem

User preferences could be used as a better input for neural collaborative filtering.

## Try feature-engineering and using an XGboost model
