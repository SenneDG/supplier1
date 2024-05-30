package com.example.supplier_1.domain;

import java.util.Objects;
import java.util.UUID;

public class ShopItem {
    private UUID id;
    public String supplier = "Supplier 2";
    private String name;
    private String description;
    private double price;
    private int quantity;
    private String imageUrl;
    private boolean isLocked;
    private int lockedQuantity;

    // getters and setters
    public UUID getId() {
        return id;
    }

    public void setId(UUID id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public double getPrice() {
        return price;
    }

    public void setPrice(double price) {
        this.price = price;
    }

    public int getQuantity() {
        return quantity;
    }

    public void setQuantity(int quantity) {
        this.quantity = quantity;
    }

    public String getImageUrl() {
        return imageUrl;
    }

    public void setImageUrl(String imageUrl) {
        this.imageUrl = imageUrl;
    }

    public boolean isLocked() {
        return isLocked;
    }

    public void setLocked(boolean locked) {
        isLocked = locked;
    }

    public int getLockedQuantity() {
        return lockedQuantity;
    }

    public void setLockedQuantity(int lockedQuantity) {
        this.lockedQuantity = lockedQuantity;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        ShopItem shopItem = (ShopItem) obj;
        return id.equals(shopItem.id);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, name, description, price, quantity, imageUrl);
    }
}