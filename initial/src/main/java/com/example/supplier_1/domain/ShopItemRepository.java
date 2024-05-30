package com.example.supplier_1.domain;

import org.springframework.stereotype.Component;

import jakarta.annotation.PostConstruct;

import java.util.*;

@Component
public class ShopItemRepository {

    private final Map<UUID, ShopItem> shopItems = new HashMap<>();

    private static final UUID ITEM1_UUID = UUID.fromString("123e4567-e89b-12d3-a456-426614174003");
    private static final UUID ITEM2_UUID = UUID.fromString("123e4567-e89b-12d3-a456-426614174004");
    private static final UUID ITEM3_UUID = UUID.fromString("123e4567-e89b-12d3-a456-426614174005");
    private static final UUID ITEM4_UUID = UUID.fromString("123e4567-e89b-12d3-a456-426614174006");
    private static final UUID ITEM5_UUID = UUID.fromString("123e4567-e89b-12d3-a456-426614174007");

    @PostConstruct
    public void init() {
        ShopItem item1 = new ShopItem();
        item1.setId(ITEM1_UUID);
        item1.setName("COCA-COLA 33CL");
        item1.setDescription("Coca-Cola 33cl is een verfrissende en iconische frisdrank die bekendstaat om zijn unieke smaak en bruisende karakter. Perfect voor elke gelegenheid en heerlijk koel geserveerd.");
        item1.setPrice(1.50);
        item1.setQuantity(20);
        item1.setImageUrl("https://goedkoopdrank.be/wp-content/uploads/2023/06/GD60.jpg");

        ShopItem item2 = new ShopItem();
        item2.setId(ITEM2_UUID);
        item2.setName("SPA BLÅ 50CL");
        item2.setDescription("Spa Blå 50cl is een verfrissend natuurlijk mineraalwater uit de Belgische Ardennen. Het staat bekend om zijn zuiverheid en zachte smaak, ideaal om op elk moment van de dag van te genieten.");
        item2.setPrice(1.00);
        item2.setQuantity(30);
        item2.setImageUrl("https://www.dirckiii.nl/media/catalog/product/cache/d55535bea25a3d0676d7fc260feb4497/8/f/8f1294769bfdbd1447ca63d781f13f0c4bfae269_5410013126737_Spa_Reine_Sport.jpg");

        ShopItem item3 = new ShopItem();
        item3.setId(ITEM3_UUID);
        item3.setName("ORANGINA 25CL");
        item3.setDescription("Orangina 25cl is een verfrissende frisdrank met een unieke mix van sinaasappelsap en echte vruchtvlees. Het staat bekend om zijn licht bruisende karakter en heerlijke, natuurlijke smaak. Perfect voor een verfrissende pauze.");
        item3.setPrice(1.75);
        item3.setQuantity(15);
        item3.setImageUrl("https://media.carrefour.fr/medias/0dcdea4544113db486517310cc4ede47/p_540x540/03249760013319-h1l1-s12.jpg");


        shopItems.put(item1.getId(), item1);
        shopItems.put(item2.getId(), item2);
        shopItems.put(item3.getId(), item3);
    }

    public Optional<ShopItem> findShopItem(UUID id) {
        return Optional.ofNullable(shopItems.get(id));
    }

    public Collection<ShopItem> getAllShopItems() {
        return shopItems.values();
    }

    public void addShopItem(ShopItem shopItem) {
        shopItems.put(shopItem.getId(), shopItem);
    }

    public void updateShopItem(UUID id, ShopItem updatedShopItem) {
        if (!shopItems.containsKey(id)) {
            throw new IllegalArgumentException("Item with id " + id + " does not exist.");
        }

        ShopItem shopItem = shopItems.get(id);
        shopItem.setName(updatedShopItem.getName());
        shopItem.setDescription(updatedShopItem.getDescription());
        shopItem.setPrice(updatedShopItem.getPrice());
        shopItem.setQuantity(updatedShopItem.getQuantity());
        shopItem.setImageUrl(updatedShopItem.getImageUrl());
        shopItem.setLockedQuantity(updatedShopItem.getLockedQuantity());
    }

    public void deleteShopItem(UUID id) {
        shopItems.remove(id);
    }
}
