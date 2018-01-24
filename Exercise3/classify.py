import pandas as pd
from sklearn.linear_model import Perceptron
from sklearn.neighbors import KNeighborsClassifier


if __name__ == '__main__':
    training = pd.read_csv('data/vertigo_train.txt', header=None, sep='\s+')
    train_x = training[[1, 2, 3, 4, 5]]
    train_y = training[0]
    test_x = pd.read_csv('data/vertigo_predict.txt', header=None, sep='\s+')
    test_y = pd.read_csv('data/vertigo_answers.txt', header=None, sep='\s+')

    p = Perceptron(n_iter=100)
    p.fit(train_x.values.tolist(), train_y.values.tolist())
    predicted = p.predict(test_x.values.tolist())
    correctPer = len([i for i, j in zip(test_y.values.tolist(), predicted) if i == j])
    accPer = correctPer / len(predicted)

    neighbours = KNeighborsClassifier(n_neighbors=1)
    neighbours.fit(train_x, train_y)
    predictedNN = neighbours.predict(test_x)
    correctNN = len([i for i, j in zip(test_y.values.tolist(), predictedNN) if i == j])
    accNN = correctNN / len(predictedNN)

    print('Perceptron: {0:.2f}% correct'.format(accPer*100))
    print('Nearest neighbor: {0:.2f}% correct'.format(accNN*100))