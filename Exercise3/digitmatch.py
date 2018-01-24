from PIL import Image, ImageFilter
from sklearn import datasets, svm, metrics


if __name__ == '__main__':
    digits_db = datasets.load_digits()

    digits_img_list = []
    digits_list = []
    for digit in range(10):
        digit_img = Image.open('data/digits/{0}.png'.format(digit))
        digit_pix = digit_img.load()
        minX, minY, maxX, maxY = (255, 255, 0, 0)

        for x in range(digit_img.size[0]):
            for y in range(digit_img.size[1]):
                pixel = digit_pix[x, y]
                if (pixel[0] + 15 <= pixel[2]) and (pixel[1] + 15 <= pixel[2]):
                    digit_pix[x, y] = (0, 0, 0, 255)
                    maxX = x if x > maxX else maxX
                    minX = x if x < minX else minX
                    maxY = y if y > maxY else maxY
                    minY = y if y < minY else minY
                else:
                    digit_pix[x, y] = (255, 255, 255, 255)

        digit_img = digit_img.crop((minX, minY, maxX, maxY))
        square_size = maxX-minX if (maxX-minX) > (maxY-minY) else (maxY-minY)
        squared = Image.new('RGB', (square_size, square_size), color=(255, 255, 255))
        squared.paste(digit_img, (int((square_size - digit_img.size[0])/2), int((square_size - digit_img.size[1] )/2)))
        squared = squared.resize((16, 16)).convert(mode='L').filter(ImageFilter.Kernel((3, 3), [0.0625, 0.125, 0.0625,
                                                                                                0.125, 0.25, 0.125,
                                                                                                0.0625, 0.125, 0.0625]))
        squared_pix = squared.load()

        new_img_list2d = []
        for x in range(8):
            new_img_row = []
            for y in range(8):
                pix_value = (squared_pix[2*y, 2*x] + squared_pix[2*y+1, 2*x] +
                             squared_pix[2*y, 2*x+1] + squared_pix[2*y+1, 2*x+1])/4
                new_img_row.append((256 - pix_value)/16)
            new_img_list2d.append(new_img_row)

        new_img_list = [x for row in new_img_list2d for x in row]
        digits_img_list.append(new_img_list)
        digits_list.append(digit)
    ###

    n_samples = len(digits_db.images)
    data = digits_db.images.reshape((n_samples, -1))

    classifier = svm.SVC(gamma=0.001)
    classifier.fit(data, digits_db.target)

    expected = digits_list
    predicted = classifier.predict(digits_img_list)


    for (ex,pred) in zip(expected, predicted):
        print('Classified as {0} - expected {1}'.format(pred, ex))

    acc = len([i for i, j in zip(expected, predicted) if i == j]) / len(predicted)
    print('Accuracy: {0:.2f}% correct'.format(acc * 100))
