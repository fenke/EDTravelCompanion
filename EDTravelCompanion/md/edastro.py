# AUTOGENERATED! DO NOT EDIT! File to edit: ../../nbs/md/90_edastro.ipynb.

# %% auto 0
__all__ = ['github_raw_base_url', 'ed_astro_galmap_url', 'create_galaxymap_url', 'embed_galaxymap']

# %% ../../nbs/md/90_edastro.ipynb 3
#| export


# %% ../../nbs/md/90_edastro.ipynb 4
github_raw_base_url = "https://raw.githubusercontent.com/fenke/EDTravelCompanion/main/nbs/blog/posts/ed-guardians-1/"
ed_astro_galmap_url="https://edastro.com/galmap/"

def create_galaxymap_url(markers, **kwargs):

    if kwargs:
        ed_astro_galmap_custom_url = f"{ed_astro_galmap_url}?{'&'.join([f'{k}={v}' for k,v in kwargs.items()])}&custom="
    else:
        ed_astro_galmap_custom_url = f"{ed_astro_galmap_url}?custom="

    if isinstance(markers, list):
        source = f"{ed_astro_galmap_custom_url}{';'.join([github_raw_base_url + str(s) for s in markers])}"
    else:
        source = f"{ed_astro_galmap_custom_url}{github_raw_base_url + str(markers)}"

    return source 


# %% ../../nbs/md/90_edastro.ipynb 5
def embed_galaxymap(markers, **kwargs):
    return f''' <iframe width=100% height=800 src="{create_galaxymap_url(markers, **kwargs)}"></iframe> '''

