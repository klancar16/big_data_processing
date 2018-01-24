import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
from scipy import stats
from scipy.optimize import curve_fit


def func(x, a, b, c):
    return a * np.exp(-b * x) + c


if __name__ == '__main__':
    dat = pd.read_csv("data/vehicle_data.csv", delimiter=';', encoding='ANSI',
                      usecols=['ajoneuvoluokka', 'ensirekisterointipvm', 'Co2'])
    # dat = pd.read_csv("Tieliikenne AvoinData 4.10.csv", delimiter=';', encoding='ANSI')
    cars = dat[dat.ajoneuvoluokka.isin(['M1', 'M1G'])]
    cars = cars[['ensirekisterointipvm', 'Co2']]
    cars.dropna(axis=0, inplace=True)
    cars['ensirekisterointipvm'] = cars['ensirekisterointipvm'].apply(lambda x: int(x[:4]))

    co_and_count_by_year = pd.pivot_table(cars, values='Co2', index='ensirekisterointipvm', aggfunc=[np.average, len])

    # scatter plot
    co_by_year = pd.pivot_table(cars, values='Co2', index='ensirekisterointipvm', aggfunc=np.average)
    plt.scatter(co_by_year.index, co_by_year)
    plt.show()
    ###

    slope, intercept, _, _, _ = stats.linregress(x=co_by_year.index.values, y=co_by_year.Co2.values)

    poly_fit = np.polyfit(x=co_by_year.index.values, y=co_by_year.Co2.values, deg=3)

    # change index from year to year - min_year for curve fitting
    min_year = min(co_by_year.index)
    popt, pcov = curve_fit(func, co_by_year.index - min_year + 1, co_by_year.Co2.values, maxfev=2500)
    limited_data = co_by_year.loc[co_by_year.index >= 2008]
    popt_lim, pcov_lim = curve_fit(func, limited_data.index - 2008 + 1, limited_data.Co2.values, maxfev=2500)

    years = np.linspace(min_year, 2030, 2030 - min_year)
    # regression
    plt.plot(years, intercept + slope * years, 'g-', label='regression')
    # polyfit
    polyfit_pred = np.poly1d(poly_fit)
    plt.plot(years, polyfit_pred(years), 'b-', label='polyfit')
    #curve fit
    plt.plot(years, func(years - min_year + 1, *popt), 'r-', label='curve fit')
    lim_years = np.linspace(2008, 2030, 2030 - 2008)
    plt.plot(lim_years, func(lim_years - 2008 + 1, *popt_lim), 'c-', label='curve fit limited')

    plt.xlabel('Year')
    plt.ylabel('Average emissions [g/km]')
    plt.legend()
    plt.savefig('data/emissions.png')

    # print results
    print('2005: {0:.3f} g/km'.format(co_by_year.loc[2005].values[0]))
    print('2010: {0:.3f} g/km'.format(co_by_year.loc[2010].values[0]))
    print('2015: {0:.3f} g/km'.format(co_by_year.loc[2015].values[0]))

    print('----------------------')
    print('Regression:')
    print('2020: {0:.3f} g/km'.format(intercept + slope * 2020))
    print('2025: {0:.3f} g/km'.format(intercept + slope * 2025))
    print('2030: {0:.3f} g/km'.format(intercept + slope * 2030))

    print('----------------------')
    print('Polyfit:')
    print('2020: {0:.3f} g/km'.format(polyfit_pred(2020)))
    print('2025: {0:.3f} g/km'.format(polyfit_pred(2025)))
    print('2030: {0:.3f} g/km'.format(polyfit_pred(2030)))

    print('----------------------')
    print('Curvefit:')
    print('2020: {0:.3f} g/km'.format(func(2020 - min_year + 1, *popt)))
    print('2025: {0:.3f} g/km'.format(func(2025 - min_year + 1, *popt)))
    print('2030: {0:.3f} g/km'.format(func(2030 - min_year + 1, *popt)))

    print('----------------------')
    print('Curvefit limited:')
    print('2020: {0:.3f} g/km'.format(func(2020 - 2008 + 1, *popt_lim)))
    print('2025: {0:.3f} g/km'.format(func(2025 - 2008 + 1, *popt_lim)))
    print('2030: {0:.3f} g/km'.format(func(2030 - 2008 + 1, *popt_lim)))




