import numpy as np

class Classifier():

    def __init__(self):
        pass

    def sigmoid(self, x):
        x1, x2 = x
        return 1./(1. + np.exp(0.897*x1-0.234*x2))

    def predict(self, x):
        try:
            x = np.array(x, dtype=float)
            prediction = np.round(self.sigmoid(x))
            return int(prediction)
        except:
            return -1

def main():
    pass

if __name__ == '__main__':
    main()
