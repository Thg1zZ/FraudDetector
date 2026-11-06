package dev.thiagogomes.fraud.simulator;

import java.util.List;

record City(String name, double latitude, double longitude) {

    static final List<City> ALL = List.of(
            new City("São Paulo", -23.55, -46.63),
            new City("Rio de Janeiro", -22.91, -43.17),
            new City("Belo Horizonte", -19.92, -43.94),
            new City("Curitiba", -25.43, -49.27),
            new City("Porto Alegre", -30.03, -51.23),
            new City("Salvador", -12.97, -38.50),
            new City("Recife", -8.05, -34.88),
            new City("Fortaleza", -3.73, -38.52),
            new City("Brasília", -15.79, -47.88),
            new City("Manaus", -3.12, -60.02),
            new City("Lisboa", 38.72, -9.14),
            new City("Miami", 25.76, -80.19));
}
