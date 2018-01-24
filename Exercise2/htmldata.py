import pandas as pd
import matplotlib.pyplot as plt

urls = [
    "https://en.wikipedia.org/wiki/List_of_countries_by_GDP_(PPP)_per_capita",
    "https://en.wikipedia.org/wiki/World_Happiness_Report",
    "https://en.wikipedia.org/wiki/List_of_countries_by_life_expectancy",
    "https://en.wikipedia.org/wiki/List_of_sovereign_states_and_dependent_territories_by_birth_rate"
]

if __name__ == '__main__':
    per_capita = pd.read_html(urls[0], header=0, index_col=0)[1]
    per_capita.index = per_capita['Country/Territory']
    per_capita = per_capita['Int$']

    happiness = pd.read_html(urls[1], header=0, index_col=0)[0]
    happiness.index = happiness['Country']
    happiness = happiness['Score']

    life_expect = pd.read_html(urls[2], header=0, index_col=0)[0]
    life_expect = life_expect['Both sexes life expectancy']

    birth_rate = pd.read_html(urls[3], header=0, index_col=0)[1]
    birth_rate = birth_rate['CIA WF 2016']

    result = pd.concat([per_capita, happiness, life_expect, birth_rate], join='inner', axis=1)
    result.columns.name = 'Country'
    result.sort_index(inplace=True)
    result = result.apply(pd.to_numeric)

    result.to_html('data/countrydata.html')

    # graphs
    plt.rcParams["figure.figsize"] = [16.0, 12.0]
    plt.subplot(321)
    plt.scatter(result['Int$'], result['Score'])
    plt.xlabel('GDP')
    plt.ylabel('Happiness')

    plt.subplot(322)
    plt.scatter(result['Int$'], result['Both sexes life expectancy'])
    plt.xlabel('GDP')
    plt.ylabel('Life expectancy')

    plt.subplot(323)
    plt.scatter(result['Int$'], result['CIA WF 2016'])
    plt.xlabel('GDP')
    plt.ylabel('Birth rate')

    plt.subplot(324)
    plt.scatter(result['Score'], result['Both sexes life expectancy'])
    plt.xlabel('Happiness')
    plt.ylabel('Life expectancy')

    plt.subplot(325)
    plt.scatter(result['Score'], result['CIA WF 2016'])
    plt.xlabel('Happiness')
    plt.ylabel('Birth rate')

    plt.subplot(326)
    plt.scatter(result['Both sexes life expectancy'], result['CIA WF 2016'])
    plt.xlabel('Life expectancy')
    plt.ylabel('Birth rate')

    plt.savefig('data/countryplots.png')



