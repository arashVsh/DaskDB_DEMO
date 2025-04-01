import matplotlib.pyplot as plt
from sklearn.linear_model import LinearRegression
from sklearn.cluster import KMeans
import numpy as np

def myPlot(df):
    df.plot()
    plt.show()
    
def plotBarGraph(df):
    columns = df.columns
    x = df[columns[0]].values.tolist()
    y = df[columns[1]].values.tolist()
    fig = plt.figure(figsize = (10, 5))
    plt.bar(x, y)
    plt.xlabel(columns[0])
    plt.ylabel(columns[1])
    plt.show()
    
def myLinearFit(s1,s2):
    model = LinearRegression().fit(np.array(s1).reshape(-1,1), s2)
    pred = model.predict(np.array(s1).reshape(-1,1))
    plt.scatter(s1, s2, color='black')
    plt.plot(s1,pred,color='blue', linewidth=3)
    plt.xlabel(list(s1.columns)[0])
    plt.ylabel(list(s2.columns)[0])
    plt.title("Plot for Linear Regression")
    plt.show()
    
def myKMeans(df):
    kmeans = KMeans(n_clusters=4).fit(df)
    centroids = kmeans.cluster_centers_
     
    col1 = list(df.columns)[0]
    col2 = list(df.columns)[1]
    plt.scatter(df[col1], df[col2], c= kmeans.labels_.astype(float), s=50, alpha=0.5)
    plt.scatter(centroids[:, 0], centroids[:, 1], c='red', s=50)
    plt.xlabel(col1)
    plt.ylabel(col2)
    plt.title("Plot for K-Means")
    plt.show()        
                