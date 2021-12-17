# airfare-price-prediction

Introduction

Generally, the earlier you buy the plane tickets, the cheaper it is. But this is not always true, low-budget airlines tend to offer more deals and hence they are cheaper on certain dates. Our goal is to develop a model that predicts the flight prices. We do this by analysing historical data. Subsequently the project will also analyse the impact of COVID-19 on flight prices. 
Airlines use a very sophisticated approach ”Revenue Management” or “Yield Management” to increase flight fare based on, time of purchase patterns and increasing the fares of a flight as the number of seats run out.


Problem Statement

Flight prices are very difficult to guess as they vary every day due to their sophisticated algorithm but as a data scientist given the right data which has an influence on the flight fare, can predict an affordable flight fare.


Data Description

The dataset consists of a relational database representing flight fares on various routes across the United States over 4 different quarters from 1996 – 2021.
The dataset has the following columns 
Year: This column represents the year of travel.
Quarter: Represents the quarter of a year.
Origin: The city where the flight departed.
Destination: The city where the flight arrived.
Average Fare: Average fare for that route.
Distance: Distance between the origin and destination city
Carrier_lg: Carrier with largest market share
Carrier_low: Carrier with lowest fare in that route


Normalization

Normalization is a process of organizing data into a relational database, also helps in reducing the redundancy from set of relations in a relational database.
Creation of 5 tables were done Years, Quarters, Carriers, Airports, Airfares. All the tables were created with an autoincremented ID primary key, the table Years, Quarters, Carriers, Airports have an additional column Year, Quarter, Code and Name, respectively.

The Airfares relational database includes all of these above-mentioned unique ID additionally with its unique ID, origin, destination, Average fare, where origin and destination refer to Airport’s ID.

We fetched data from an csv file and inserted only the unique values into these 4 tables, Year, Quarter, Carrier and Airport. Finally, the Airfares table was populated with all the data from individual tables and made an inner join to all the tables on their ID values. 
The final relational database was created without any redundant or repeated values, and with an inner join to other tables.
The relational database was used for visualization and for predicting the air fare.


Data Visualization

Data visualization is done in order to view the huge data we plot them as graphs to better understand the patter of the data which might be very useful for any machine learning algorithm also for further analysis. As humans are used to grasp a lot of information when data is represented diagrammatically.

Feature selection and Pre-processing 

Feature selection is another important step required for selecting only those columns which have more influence on the model’s prediction compared to other columns in the dataset.


Prediction

The prediction of the affordable flight fare was done by implementing machine learning algorithm – Random Forest Regression
Random Forest Regression, is a learning method for both classification and regression problems. It is an ensemble of decision trees which is usually trained by bagging method. For classification, the output is the class selected by most trees whereas for regression problem the mean of prediction of all individual trees are returned. Some features of Random Forest are, it can run efficiently on large datasets, it can also provide us the most important features for the model, also maintains accuracy for large number of missing values in the dataset, it can also handle lot of input variables without deletion. 


Conclusion

The dataset from a csv file was populated into a relational database by the process of normalization. This relational database was then used for further analysis and prediction. The visualization helped in better analyzing the data, for better interpretation. Finally, for prediction of average fare we used random forest regressor model, which yielded a test accuracy of 92.88%.

 
