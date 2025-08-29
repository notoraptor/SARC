# 1) Parsing des nodes->GPU

Le dossier `sarc-cache` contient les fichiers slurm.conf que j'avais récupérés, sur les derniers mois.
Malheureusement, les fichiers slurm.conf sont modifiés sans aucun indice sur leur date de modification,
donc il se peut que j'ai manqué des changements.

Ća pourrait être souhaitable de copier ces fichiers dans le cache local de sarc,
puis appeler `sarc acquire slurmconfig` pour chaque fichier.

Le script `acquire_slurmconfig.sh` permet de parser tous les fichiers slurm.conf ci-joints
s'ils sont bien dans le cache local. Contenu du script:

```bash
SARC_MODE=scraping SARC_CONFIG=config/sarc-dev.yaml uv run sarc -v acquire slurmconfig -c beluga -d 2024-04-03
SARC_MODE=scraping SARC_CONFIG=config/sarc-dev.yaml uv run sarc -v acquire slurmconfig -c cedar -d 2024-04-03
SARC_MODE=scraping SARC_CONFIG=config/sarc-dev.yaml uv run sarc -v acquire slurmconfig -c cedar -d 2025-02-26
SARC_MODE=scraping SARC_CONFIG=config/sarc-dev.yaml uv run sarc -v acquire slurmconfig -c cedar -d 2025-07-03
SARC_MODE=scraping SARC_CONFIG=config/sarc-dev.yaml uv run sarc -v acquire slurmconfig -c graham -d 2024-04-03
SARC_MODE=scraping SARC_CONFIG=config/sarc-dev.yaml uv run sarc -v acquire slurmconfig -c graham -d 2025-03-02
SARC_MODE=scraping SARC_CONFIG=config/sarc-dev.yaml uv run sarc -v acquire slurmconfig -c graham -d 2025-03-20
SARC_MODE=scraping SARC_CONFIG=config/sarc-dev.yaml uv run sarc -v acquire slurmconfig -c graham -d 2025-07-03
SARC_MODE=scraping SARC_CONFIG=config/sarc-dev.yaml uv run sarc -v acquire slurmconfig -c mila -d 2024-10-01
SARC_MODE=scraping SARC_CONFIG=config/sarc-dev.yaml uv run sarc -v acquire slurmconfig -c mila -d 2025-07-01
SARC_MODE=scraping SARC_CONFIG=config/sarc-dev.yaml uv run sarc -v acquire slurmconfig -c narval -d 2023-11-28
SARC_MODE=scraping SARC_CONFIG=config/sarc-dev.yaml uv run sarc -v acquire slurmconfig -c narval -d 2025-02-26
SARC_MODE=scraping SARC_CONFIG=config/sarc-dev.yaml uv run sarc -v acquire slurmconfig -c narval -d 2025-03-20
```

# 2) Correction des GPU jobs dont le GPU type est None

Le script `fix_gpu_jobs_without_gpu_type.py` permet de détecter et corriger les GPU jobs sans GPU type.

Par défaut, il détecte les GPU jobs et vérifie s'il peut les corriger, sans les sauvegarder.
Ća peut être utile pour vérifier la base de données sans la modifier (lecture seule):

```bash
SARC_MODE=scraping SARC_CONFIG=config/sarc-dev.yaml uv run fix_gpu_jobs_without_gpu_type.py
```

Pour appliquer les corrections à la DB (écriture), il faut appeler le script avec l'argument `save`:
```bash
SARC_MODE=scraping SARC_CONFIG=config/sarc-dev.yaml uv run fix_gpu_jobs_without_gpu_type.py save
```
