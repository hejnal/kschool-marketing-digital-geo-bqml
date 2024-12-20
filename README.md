# kschool-marketing-digital-geo-bqml
The repository contains artefacts necessary for the module development.

The basic structure is:
- data (raw data to be ingested to BigQuery)
- notebooks - detailed instructions for each lab (with solutions)
- sql - basic repository of the sql queries used during the sessions

`hint`: replace <project_id> with your own project_id and <dataset_id> with your own dataset_id (no hyphens).

Example:

```bash
egrep -rl '<project_id>' ./ | grep -v README.md | xargs -I@ sed -i '' "s/<project_id>/whejna-bqml-sandbox/g" @

egrep -rl '<dataset_id>' ./ | grep -v README.md | xargs -I@ sed -i '' "s/<dataset_id>/raw_data/g" @

egrep -rl '<table_name>' ./ | grep -v README.md | xargs -I@ sed -i '' "s/<table_name>/spotify_dataset/g" @
```
