- Worked with a large-scale dataset (~6.8M reviews, ~800k products, ~23k brands) to understand the jewelry market on Amazon.

- Cleaned and structured the data to analyze products, brands, prices, ratings, and review texts.

- Compared brand performances:

    - Aggregated product ratings for each brand, used a weighted score to avoid bias from products with very few reviews.
    - Ranked ~11k+ brands based on both rating and review volume.

<img width="760" height="470" alt="image" src="https://github.com/user-attachments/assets/6da25d80-56cc-4d1e-894c-351158cb8f36" />


- Analyzed how brands perform over time (2004–2023):
    - Tracked yearly brand rankings.
    - Observed how top brands change over time.
    - Followed the evolution of large brands (e.g. Amazon Collection) in terms of rating and popularity.
      
<img width="756" height="393" alt="image" src="https://github.com/user-attachments/assets/2e9d7c39-1301-406b-a930-bfdf8b96f575" />

- Explored price behavior in detail:

    - Analyzed full price distribution (highly skewed, long tail up to ~33k $).
  
    <img width="613" height="475" alt="image" src="https://github.com/user-attachments/assets/8e12db80-3829-404c-9643-3c8b24b0e428" />

    <img width="613" height="456" alt="image" src="https://github.com/user-attachments/assets/b4ecc8b2-aa65-4129-94ed-bd9e8b8923d3" />

    - Segmented products into cheap / mid / luxury.

    <img width="617" height="470" alt="image" src="https://github.com/user-attachments/assets/57af04ea-173b-4c1c-ab59-0b8dadd5b811" />


- Analyzed how brand-level average prices evolve over time. For each year, the average price of products was computed at the brand level, and the resulting distribution of brand-level average prices was visualized using boxplots, segmented into price categories.

<img width="1012" height="525" alt="image" src="https://github.com/user-attachments/assets/ddf1621b-b359-4b53-a0b4-a2290145881b" />

- Investigated relationship between price and satisfaction:

    - Tested whether expensive brands are actually better rated. Compared how ratings differ across these price segments. All segments have very similar rating ranges, but cheap products show a wider spread, and luxury products are more tightly clustered.

        <img width="691" height="547" alt="image" src="https://github.com/user-attachments/assets/de61f600-c15a-4c0e-abb8-fa6a6319df19" />
 
    - Built different versions of a “value for money” score (including log and z-score normalization).
        - Results depend heavily on the definition. The results of the rating / log(price) approach favor cheap products. Using normalized score (z-score) leads to more balanced recommendations.
      
        The higher the score value, the better  
        <img width="768" height="552" alt="image" src="https://github.com/user-attachments/assets/ede62207-8cc9-4ad5-b2b7-b932ef922df1" />

        The higher the z score, the better
        <img width="768" height="552" alt="image" src="https://github.com/user-attachments/assets/b1f40fd8-7bb6-4b07-94b2-014868c2b445" />


- Analyzed market structure:

    - Showed strong imbalance: ~108k cheap products vs only ~300 luxury ones.

    - Compared average rating and popularity (review counts) across price segments.
        - The plot shows that the ratings across all segments are relatively stable over the years, with cheap and mid-priced products slightly improving over time, while the luxury products show more fluctuation due to a smaller sample size.
 
        <img width="691" height="546" alt="image" src="https://github.com/user-attachments/assets/2e21e4b3-95a1-4e14-9c97-79741088b2d9" />

        - Cheap products have the highest number of ratings, which is due to higher customer engagement and volume. Luxury products remain significantly less reviewed but show gradual growth in recent years. This suggests increasing interest in higher-end items.

        <img width="694" height="568" alt="image" src="https://github.com/user-attachments/assets/00541a4b-3c86-4a67-bdc4-41f50595588e" />


- Performed review text analysis (NLP):

    - Processed reviews (tokenization, stopwords, etc)

    - Generated word distributions and word clouds.
  
      Example:
      
    <img width="515" height="290" alt="image" src="https://github.com/user-attachments/assets/851e1f14-524d-49f0-9c42-74bebf0ae30d" />

    - Tracked word trends over time and across brands.

    <img width="508" height="375" alt="image" src="https://github.com/user-attachments/assets/06d8ba02-35cb-4d34-bb7e-9a82900cdc94" />

- Analyzed how certain keywords/phrases are distributed across brands or segments.

    - Luxury products receive more price-related language, both positive and negative. It suggests luxury buyers talk more about value for money in both directions.

    <img width="479" height="340" alt="image" src="https://github.com/user-attachments/assets/ba18a3fa-5380-4d7d-b4e4-90dd13e0b6fb" />

    <img width="488" height="340" alt="image" src="https://github.com/user-attachments/assets/b6ae3df2-8e02-4877-9251-bfa5469ad0c9" />

    - Affordable/budget wording is concentrated in cheap products and almost absent in luxury.

    <img width="488" height="340" alt="image" src="https://github.com/user-attachments/assets/be9f2d55-9d08-4ebe-b352-12878e9c87dc" />

    - Compared how often specific complaint keywords appear across price segments. It shows the highest frequency for cheap products. It supports the idea that lower-priced items get more complaints about physical failure, visible damage/durability.

    <img width="488" height="340" alt="image" src="https://github.com/user-attachments/assets/7279f543-92f9-4c16-bd0b-c35b15f2d99c" />

    <img width="488" height="340" alt="image" src="https://github.com/user-attachments/assets/63286ba2-90f2-4640-9e28-33f850ef64ce" />

    - Cheap products fail physically more often, but luxury products are judged more critically in terms of perceived quality. It reflects higher customer expectations and stricter evaluation standards.

    <img width="488" height="340" alt="image" src="https://github.com/user-attachments/assets/4c4cf80f-02ae-4e73-af33-498f9a19f00e" />

    - Luxury products are more often associated with size/fit complaints.

    <img width="487" height="340" alt="image" src="https://github.com/user-attachments/assets/1245da80-d4e1-4c1b-ac14-db1767c71c5c" />

    <img width="487" height="340" alt="image" src="https://github.com/user-attachments/assets/720921b8-985f-4d29-a7a2-458d252b1056" />

    <img width="488" height="340" alt="image" src="https://github.com/user-attachments/assets/57aba12f-a103-44ae-bd41-60a916a94fd5" />

    - Higher-priced products are more often praised for their appearance. On the other hand, lower-priced products are more often criticized for not matching expectations.

     <img width="488" height="340" alt="image" src="https://github.com/user-attachments/assets/dad91741-d3f8-4cc3-a73d-77beaf37ac47" />

    <img width="488" height="340" alt="image" src="https://github.com/user-attachments/assets/3f943ae1-a27d-4feb-9dbd-4095c9e8626a" />

    - Comfort/usability complaints appear highest in cheap, lowest in mid, with luxury in between.

    <img width="488" height="340" alt="image" src="https://github.com/user-attachments/assets/a151f742-17df-482d-84d4-177394c1a515" />



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

    - “Quality problems” are far less common in luxury products. 

    - On the other hand, size-related complaints are more noticeable in luxury items.
 
    - Complaint patterns reveal a gap between quality and customer expectations. Cheaper products tend to have more physical failures (e.g., broken, defects). Luxury products, on the other hand, more often contains subjective dissatisfaction (e.g., cheaply made) or focuses more on value for money and expectations. As a result, customers tend to judge luxury items more critically.

    - Luxury reviews emphasize value-for-money discussions, while cheap product reviews emphasize affordability, showing that price plays a different psychological role across segments.

- Brand rankings are highly dynamic over time. The yearly analysis shows that:

    - Top-ranked brands change frequently.
 
    - Market competition increases over time with more brands entering.
 
    - Certain brands show exceptionally high association with specific complaints in a price segment (e.g., up to ~65–78% of reviews mentioning breakage in cheap products). It reveals significant quality differences within the same price segment. 

    - Many top brands in a given year have only a few products.

