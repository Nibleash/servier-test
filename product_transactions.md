# Deuxième partie : SQL

**Objectif** : : Réaliser des requêtes SQL claires et facilement compréhensibles.

## Chiffre d'affaires par jour

Réaliser une requête SQL simple permettant de trouver le chiffre d’affaires (le montant total des ventes),
jour par jour, du 1er janvier 2019 au 31 décembre 2019. Le résultat sera trié sur la date à laquelle la commande a été passée.

```sql
SELECT
    t.date,
    SUM(t.prod_qty * t.prod_price) AS ventes
FROM
    transactions t
WHERE
    t.date BETWEEN '01/01/19' AND '12/31/19'
GROUP BY
    t.date
ORDER BY
    t.date ASC;
```

L'intérêt de cette requête réside dans l'utilisation de la fonction d'agrégation `SUM(t.prod_qty * t.prod_price)`.
En effet, on calcule ici le chiffre d'affaires quotidien en multipliant pour chaque vente la quantité du produit (*t.prod_qty*)
par son prix unitaire (*t.prod_price*), puis en sommant ces valeurs pour toutes les ventes d'un jour distinct.
Les résultats sont ensuite regroupés par date et triés de manière ascendante.

## Ventes par client par type de produit

Réaliser une requête un peu plus complexe qui permet de déterminer, par client et sur la période allant
du 1er janvier 2019 au 31 décembre 2019, les ventes meuble et déco réalisées.

```sql
SELECT
  t.client_id,
  SUM(
    CASE WHEN pn.product_type = 'MEUBLE' THEN t.prod_qty * t.prod_price ELSE 0 END
  ) AS ventes_meubles,
  SUM(
    CASE WHEN pn.product_type = 'DECO' THEN t.prod_qty * t.prod_price ELSE 0 END
  ) AS ventes_deco
FROM
  transactions t
  JOIN product_nomenclature pn ON t.prod_id = pn.product_id
WHERE
  t.date BETWEEN '01/01/19' AND '12/31/19'
GROUP BY
  t.client_id
ORDER BY
  t.client_id DESC;
```

Pour cette requête, on utilise une jointure entre les tables **transactions** et **product_nomenclature** basée sur les
identifiants de produits `ON t.prod_id = pn.product_id` afin de récupérer le type de produit acheté par le client (*pn.product_type*).
On utilise ensuite une clause `CASE` imbriquée dans une clause `SUM` pour calculer les ventes relatives à chaque type de produit.
Enfin, on regroupe les résultats par client, avec des colonnes distinctes pour les ventes de meubles et de décorations, et triés de manière décroissante par identifiant de client.