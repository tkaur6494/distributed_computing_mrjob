from mrjob.job import MRJob
from sklearn.preprocessing import MinMaxScaler
import csv
from collections import Counter
import numpy as np


class MRKNNClassification(MRJob):
    # storing the training records i.e records with labels
    trainingData = []

    # reading the file
    with open('Iris.csv') as csvfile:
        line = csv.reader(csvfile)
        # skipping headers of the file
        next(line)
        for data in line:
            # filtering out training and testing records
            if(data[5] != ""):
                trainingData.append(data)

    # converting list to numpy array for normalization of the training data
    df_training = np.array(trainingData)
    normalize = MinMaxScaler().fit(df_training[:, [1, 2, 3, 4]])
    df_training[:, [1, 2, 3, 4]] = normalize.transform(
        df_training[:, [1, 2, 3, 4]])

    # mapper
    def mapper(self, _, line):
        data = line.split(",")
        # mapper receives the entire file line by line
        # filtering out only those records that do not have a label
        if(data[0] != "Id" and data[5] == ""):
            # normalizing the test data using the normalize attribute created in the class
            data_normalized = self.normalize.transform(
                np.array([data[1:5]]))[0].tolist()
            data = [data[0], data_normalized[0], data_normalized[1],
                    data_normalized[2], data_normalized[3]]
            # storing euclidian distance
            eucldian_distance = []
            # computing euclidian distance with respect to every row of the training data
            # euclidian distance = [{label:"Iris viriginica",value:10}] -> example format
            for row_train in range(0, len(self.df_training)):
                eucldian_distance.append(
                    {"label": self.df_training[row_train][5], "value": 0})
                # considering only the features from 1rst-4th column
                for col_test in range(1, 5):
                    # formula = (p1-q1)^2 + (p2-q2)^2 + (p3-q3)^2 + (p4-q4)^2
                    eucldian_distance[row_train]["value"] = eucldian_distance[row_train]["value"] + (
                        float(data[col_test]) - float(self.df_training[row_train][col_test]))**2
                eucldian_distance[row_train]["value"] = eucldian_distance[row_train]["value"]**0.5
            # getting the top 15 neighbors for KNN
            eucldian_distance = sorted(
                eucldian_distance, key=lambda i: (i['value']))[:15]
            # returns id and top 15 neighbors
            yield(data[0], eucldian_distance)

    def reducer(self, id, neighbors):
        class_label = []
        # storing all labels in a list
        for elem in list(neighbors)[0]:
            class_label.append(elem["label"])
        # Using Counter library to get the value of the most common label present
        max_frequency = Counter(class_label).most_common(1)
        # returns id and label
        yield (id, max_frequency[0][0])


if __name__ == '__main__':
    MRKNNClassification.run()
