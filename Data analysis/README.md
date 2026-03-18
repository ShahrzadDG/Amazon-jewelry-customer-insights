- Worked with a large-scale dataset (~6.8M reviews, ~800k products, ~23k brands) to understand the jewelry market on Amazon.

- Cleaned and structured the data to analyze products, brands, prices, ratings, and review text.

- Compared brands performances:

    - Aggregated product ratings for each brand, used a weighted score to avoid bias from products with very few reviews.
    - Ranked ~11k+ brands based on both rating quality and review volume.

<img width="915" height="624" alt="image" src="https://github.com/user-attachments/assets/f2b8afcd-419c-491c-b640-80835b5ccbe4" />


- Analyzed how brands perform over time (2004–2023):
        - Tracked yearly brands rankings.
        - Observed how top brands change over time.
        - Followed the evolution of large brands (e.g. Amazon Collection) in terms of rating and popularity.
  <img width="717" height="470" alt="image" src="https://github.com/user-attachments/assets/76f44052-b030-4f78-91d5-50af61f26656" />

- Explored price behavior in detail:

  analyzed full price distribution (highly skewed, long tail up to ~33k)

  segmented products into cheap / mid / luxury

  compared how ratings differ across these segments over time

Investigated relationship between price and satisfaction:

  compared average ratings per price segment

  tested whether expensive brands are actually better rated

  built different versions of a “value for money” score (including log and z-score normalization)

Analyzed market structure:

  showed strong imbalance: ~108k cheap products vs only ~300 luxury ones

  compared popularity (review counts) across segments

Performed review text analysis (NLP):

  processed ~1M+ reviews (tokenization, stopwords, normalization)

  generated word distributions and word clouds

  tracked word trends over time and across brands

  compared how often specific complaint keywords appear across price segments

Part of the results:

The market is extremely skewed toward cheap products

  Cheap items dominate both in number of products and number of reviews (~100k+ cheap vs only a few hundred luxury).
  
  This suggests a highly price-driven and competitive market.

Higher price does not consistently lead to higher ratings

  When comparing average ratings across price segments over time, expensive products are not clearly better rated than cheaper ones.
  
  Customer satisfaction seems to depend more on expectations and perceived value than on price itself.

Customer complaints follow clear patterns across segments

  From your keyword analysis (normalized by number of reviews):

  “quality problems” are more common in cheap products (~5.4%) than mid (~4.7%) and luxury (~2%)

  some issues (like defects) appear more evenly across segments

size-related complaints exist in all segments, but are relatively noticeable in luxury as well

This shows that:

  cheap products struggle more with quality/durability

  but expectation mismatch exists in all price levels

  Brand rankings are highly dynamic over time
  
The yearly analysis shows that:

  top-ranked brands change frequently

  many top brands in a given year have only a few products

  large brands (like Amazon Collection) evolve gradually rather than dominating constantly


“Best value” strongly depends on how value is defined

When using raw rating/price, extremely cheap products dominate

When using normalized scores (z-score), more balanced brands appear

This shows that:

  “value for money” is not trivial

  results depend heavily on the definition, which is an important analytical insight itself
