# Trendyol Data Bootcamp Apache Flink Course Material

This repo contains an example and a homework used in the Apache Flink course for Trendyol Data Bootcamp.

## Homework
<ul>
<li>Likelihod ratios are calculated by using userId and productId pairs.</li>
<li>The job produces output as (userId, productId, purchaseRatio).</li>
</ul>

### Example output
Let say, a user with userID 1, made AddToFavorites and AddToBasket with the Product of ID 3. <br/> 
The final likelihood to purchase will be (0.7 + 0.4)/2 = 0.55 <br />
PurchaseLikelihood(1, 3, 0.55) 


### Streaming output from job
<img src="https://github.com/ahmetlekesiz/data-bootcamp-flink/blob/main/img/example_output.PNG?raw=true" />