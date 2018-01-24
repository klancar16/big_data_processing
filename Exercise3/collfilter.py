import pandas as pd
from scipy.spatial.distance import squareform, pdist
import numpy as np
from numpy import unravel_index

if __name__ == '__main__':
    dat = pd.read_csv('data/lastfm-matrix-germany.csv', delimiter=',')
    dat = dat.drop(['user'], axis=1)
    artist_dist = 1 - squareform(pdist(dat.T, "cosine"))
    users_dist = 1 - squareform(pdist(dat, "cosine"))

    np.fill_diagonal(artist_dist, -np.inf)
    np.fill_diagonal(users_dist, -np.inf)

    art_i, art_j = unravel_index(np.nanargmax(artist_dist), artist_dist.shape)
    usr_i, usr_j = unravel_index(np.nanargmax(users_dist), users_dist.shape)

    mj_idx = dat.columns.get_loc('michael jackson')
    top_idx = np.argsort(artist_dist[mj_idx])[-10:]

    mj_sim = ['{0:.4f} {1}'.format(artist_dist[mj_idx, x], dat.columns[x]) for x in top_idx]

    final_string = ''
    final_string = final_string + '{0:.4f};{1};{2}\n'.format(artist_dist[art_i, art_j],
                                                             dat.columns[art_i], dat.columns[art_j])
    final_string = final_string + '{0:.4f};{1};{2}\n'.format(users_dist[usr_i, usr_j], usr_i, usr_j)
    final_string = final_string + '\n'.join(mj_sim)

    with open('data/collfilter.txt', 'w', encoding='UTF-8') as text_file:
        text_file.write(final_string)



