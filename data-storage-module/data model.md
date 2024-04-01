**users**
| Column      | Constraints |
|-------------|-------------|
| userId      | primary key |
| firstname   | nullable    |
| lastname    | nullable    |
| joinDate    |             |
| deviceType  | nullable    |
| age         | nullable    |
| location    | nullable    |

**user_preferences**
| Column       | Constraints |
|--------------|-------------|
| userId       | foreign key references users(userId) |
| content_type |             |

primary key  (userId, content_type)

**bid_requests**
| Column           | Constraints |
|------------------|-------------|
| bidRequestId     | primary key |
| demographic      |             |
| gender           |             |
| device           |             |
| operatingSystem  |             |
| location         |             |
| content_type     |             |
| adSpace          |             |
| adFormat         |             |

**ad_impressions**
| Column       | Constraints |
|--------------|-------------|
| adId         |             |
| userId       | foreign key references users(userId) |
| campaignId   |             |
| timestamp    |             |
| website      |             |

primary key  (adId, userId)
partition key timestamp

**clicks_and_conversions**
| Column       | Constraints |
|--------------|-------------|
| timestamp    |             |
| userId       | foreign key references users(userId) |
| campaignId   |             |
| actionType   |             |

primary key (timestamp, userId, campaignId, actionType)
partition key (timestamp, actionType) *interleaved*
