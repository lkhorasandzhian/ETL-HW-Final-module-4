# DataLens dashboard plan

Recommended dashboard blocks:

1. Applications overview:
   - total applications;
   - approval rate;
   - total requested and approved amount;
   - average credit score.
2. Risk and decisions:
   - applications by `risk_level`;
   - decision distribution by `region_code` and `product_type`.
3. Kafka streaming events:
   - events by day/hour;
   - manual review and rejected document share;
   - amount distribution by risk level.
4. DataTransfer result:
   - calls by region;
   - campaign conversion proxy by `client_response`;
   - follow-up required count.

Put screenshots into `docs/images/` and link them from the root README.
