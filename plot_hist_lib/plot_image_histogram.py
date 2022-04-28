# see https://www.geeksforgeeks.org/how-to-plot-normal-distribution-over-histogram-in-python/

# !python3 -m pip install --upgrade pip
# !python3 -m pip install --upgrade Pillow

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from PIL import Image
import scipy.stats as stats

def plotImageHistogram(image: Image):
    '''
    given an PIL.Image image, standardize it (value-mu)/std
    and plot its histogram overlayed with a 
    fitted normal distribution
    
    Example:
        image = Image.open('angel-studios.png')
        plotImageHistogram(image)

    '''
    # create a 2D np array
    frame = np.asarray(image)
    # print(f"frame.shape:{frame.shape}")

    # compute stats
    mu = np.mean(frame)
    std = np.std(frame)
    minv = np.min(frame)
    maxv = np.max(frame)
    # print(f"mu:{mu}")
    # print(f"std:{std}")
    # print(f"minv:{minv}")
    # print(f"maxv:{maxv}")

    # flatten 2-d frame into a 1-d array
    data = frame.flatten()
    # print(f"data.shape:{data.shape}")

    # standardize
    data = (data - mu) / std

    # apply threshold
    min_threshold = 0.1
    max_threshold = 0.9
    data = data[data > min_threshold]
    data = data[data < max_threshold]

    # create a pandas dataframe (only for describe)
    df = pd.DataFrame(data=data)
    df.describe()
        
    # plot the histogram
    plt.hist(data, bins=25, density=True, alpha=0.6, color='b')

    # fit a normal distribution to the data
    mu, std = stats.norm.fit (data) # mean and standard deviation
    # print(f"mu:{mu} std:{std}")

    # create a normal distribution curve
    xmin, xmax = plt.xlim()
    x = np.linspace(xmin, xmax, 100)
    p = stats.norm.pdf(x, mu, std)

    # plot the normal distribution curve
    plt.plot(x, p, 'k', linewidth=2)

    title = "Fit Values: {:.2f} and {:.2f}".format(mu, std)
    plt.title(title)

    plt.show()
    