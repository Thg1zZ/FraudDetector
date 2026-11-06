package dev.thiagogomes.fraud.simulator;

import java.math.BigDecimal;

record Card(String id, City homeCity, BigDecimal averageAmount) {
}
