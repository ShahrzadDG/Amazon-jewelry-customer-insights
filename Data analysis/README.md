- Worked with a large-scale dataset (~6.8M reviews, ~800k products, ~23k brands) to understand the jewelry market on Amazon.

- Cleaned and structured the data to analyze products, brands, prices, ratings, and review texts.

- Compared brand performances:

    - Aggregated product ratings for each brand, used a weighted score to avoid bias from products with very few reviews.
    - Ranked ~11k+ brands based on both rating and review volume.

<img width="760" height="470" alt="image" src="https://github.com/user-attachments/assets/6da25d80-56cc-4d1e-894c-351158cb8f36" />


- Analyzed how brands perform over time (2004–2023):
    - Tracked yearly brands rankings.
    - Observed how top brands change over time.
    - Followed the evolution of large brands (e.g. Amazon Collection) in terms of rating and popularity.
      
  <img width="717" height="470" alt="image" src="https://github.com/user-attachments/assets/76f44052-b030-4f78-91d5-50af61f26656" />

- Explored price behavior in detail:

    - Analyzed full price distribution (highly skewed, long tail up to ~33k $).
  
      <img width="520" height="459" alt="image" src="https://github.com/user-attachments/assets/acde308b-f334-46d9-9ed3-86c6324de3d9" />

      <img width="567" height="479" alt="image" src="https://github.com/user-attachments/assets/295ab174-d4e7-424c-a5f1-304fabbc1b02" />

    - Segmented products into cheap / mid / luxury.

      <img width="617" height="470" alt="image" src="https://github.com/user-attachments/assets/57af04ea-173b-4c1c-ab59-0b8dadd5b811" />


- Investigated relationship between price and satisfaction:

    - Tested whether expensive brands are actually better rated. Compared how ratings differ across these price segments. All segments have very similar rating ranges, but cheap products show a wider spread, and luxury products are more tightly clustered.

    <img width="691" height="547" alt="image" src="https://github.com/user-attachments/assets/de61f600-c15a-4c0e-abb8-fa6a6319df19" />
 
    - Built different versions of a “value for money” score (including log and z-score normalization).
        - Results depend heavily on the definition. The results of rating / log(price) approach dominate with cheap products. Using normalized score (z-score) leads to more balanced brands. 

- Analyzed market structure:

    - Showed strong imbalance: ~108k cheap products vs only ~300 luxury ones.

    - Compared average rating and popularity (review counts) across price segments.
        - The plot shows that the ratings across all segments are relatively stable over the years, with cheap and mid-priced products slightly improving over time, while the luxury products show more fluctuation due to a smaller sample size.
 
        <img width="846" height="568" alt="image" src="https://github.com/user-attachments/assets/ce0356e0-2203-46bc-9191-b44bdd6c135d" />

        - Cheap products have the highest number of ratings, which is due to higher customer engagement and volume. Luxury products remain significantly less reviewed but show gradual growth in recent years. It suggests increasing interest in higher-end items.

    <img width="849" height="568" alt="image" src="https://github.com/user-attachments/assets/b83caa35-a8f1-4c1b-abe3-259975948632" />


- Performed review text analysis (NLP):

    - Processed reviews (tokenization, stopwords, normalization)

    - Generated word distributions and word clouds.
  
      Ex:
      
    <img width="515" height="290" alt="image" src="https://github.com/user-attachments/assets/851e1f14-524d-49f0-9c42-74bebf0ae30d" />

    - Tracked word trends over time and across brands.

    <img width="636" height="476" alt="image" src="https://github.com/user-attachments/assets/edb01ff6-34fe-4a70-9730-bbc7f9a65e98" />

- Analyzed how certain keywords/phrases are distributed across brands or segments.

    - Some terms related to affordability and pricing appear more often in lower-priced segments, while higher-priced segments are less associated with price-related language, reflecting different customer expectations across segments.

    <img width="665" height="484" alt="image" src="https://github.com/user-attachments/assets/8c604a72-9f51-4f93-a697-e5888f9153a7" />

    - Compared how often specific complaint keywords appear across price segments.

    Examples:
  
    <img width="667" height="484" alt="image" src="https://github.com/user-attachments/assets/74abd345-482d-4f7b-99c7-7fc01df2b0c4" />

    <img width="1213" height="484" alt="image" src="https://github.com/user-attachments/assets/41c66ef7-9fb4-4416-bba5-2dbdd9343164" />

    - Identified which brands are most associated with specific words concerning complaints/praises for products in a specific price segment.

    - Ex: Which brands are most associated with “break/broken” complaints in cheap products.

        - Some brands have a significantly higher share of reviews mentioning breakage (around ~65–78%), highlighting clear differences in durability issues between brands within the same price segment.
      
    <img width="846" height="606" alt="image" src="https://github.com/user-attachments/assets/07b68520-c14f-49f1-a247-cc17fdbbf0dd" />


- Part of the results:

- The jewelry market on Amazon is extremely skewed toward cheap products.

    - Cheap items dominate both in number of products and number of reviews (~100k+ cheap vs only a few hundred luxury).
  
- Higher price does not consistently lead to higher ratings.

    - When comparing average ratings across price segments over time, expensive products are not clearly better rated than cheaper ones.
        - Customer satisfaction seems to depend more on expectations and perceived value than on price itself.

- Customer complaints follow clear patterns across segments.

    - “Quality problems” are more common in cheap products (~5.4%) than mid (~4.7%) and luxury (~2%). 

    - Some issues (like defects) appear more evenly across segments. 

    - Size-related complaints exist in all segments, but are relatively noticeable in luxury items as well.

    - People mention price and affordability much more in cheaper products, while it comes up less for higher-priced items. It shows that customers focus more on cost in the low-price segment.
  
    - This shows that:

        - Cheap products struggle more with quality/durability but expectation mismatch exists in all price levels.


- Brand rankings are highly dynamic over time. The yearly analysis shows that:

    - Top-ranked brands change frequently.

    - Many top brands in a given year have only a few products.

