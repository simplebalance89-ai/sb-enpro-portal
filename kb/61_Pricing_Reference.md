# Pricing Reference — V25

**Purpose:** Price lookup guidance when price unavailable or $0
**Updated:** February 25, 2026

---

## 1. Pricing Coverage in V25

Prices come directly from the Price column in Filtration_GPT_Filters_V25.csv. When price is $0 or missing, always direct the user to contact EnPro.

---

## 2. Price Ranges by Product Type

### Cartridge Filters (10" equivalent)
| Type | Price Range | Notes |
|------|-------------|-------|
| Meltblown PP | $8-25 | Disposable, economy |
| Pleated PP | $15-45 | Standard process |
| Pleated Microglass | $35-85 | Absolute rated |
| Membrane (PES/PTFE) | $75-200 | Sterile/critical |
| Stainless Steel | $150-500 | Reusable, high temp |

### Bag Filters
| Size | Price Range | Notes |
|------|-------------|-------|
| #1 Felt | $5-15 | Economy |
| #1 Mesh | $15-35 | Standard |
| #2 Felt | $8-25 | Economy |
| #2 Mesh | $25-50 | Standard |

### Filter Housings
| Type | Price Range | Notes |
|------|-------------|-------|
| Single Cartridge (SS) | $200-800 | 316L stainless |
| Multi-Cartridge | $500-5,000 | By element count |
| Bag Housing (SS) | $300-1,500 | Single/duplex |
| High Flow Housing | $1,000-10,000 | Large diameter |

---

## 3. Alteco Pricing (in CSV)

Alteco prices are NET to EnPro (DDP).

**Housing Examples:**
| Part Number | Description | Net Price |
|-------------|-------------|-----------|
| 50300116047 | FI 011 COD7 BASIC | $470 |
| 50300126047 | FI 012 COD7 BASIC | $510 |
| 50300136047 | FI 013 COD7 BASIC | $580 |

**Cartridge Examples:**
| Part Number | Description | Net Price |
|-------------|-------------|-----------|
| Standard 10" Meltblown | 1-100 micron | $8-15 |
| Standard 20" Meltblown | 1-100 micron | $12-22 |
| Standard 40" Meltblown | 1-100 micron | $18-35 |

---

## 4. Graver Pricing (Partial Coverage)

**TefTEC PTFE Membrane:**
| Length | 0.2 um | 0.45 um | 1.0 um |
|--------|--------|---------|--------|
| 10" | $85-110 | $80-100 | $75-95 |
| 20" | $160-200 | $150-185 | $140-175 |
| 30" | $230-285 | $215-265 | $200-250 |

**Borosilicate Glass (BC Series):**
| Length | Price Range |
|--------|-------------|
| 10" | $45-75 |
| 20" | $85-130 |
| 30" | $125-185 |

---

## 5. Vendor Contact for Quotes

When price not in CSV, direct to EnPro:

**EnPro Contact:**
- Email: service@enproinc.com
- Phone: 1 (800) 323-2416
- Response: Usually within 24 hours

**Quote Requirements:**
1. Part number (EnPro or OEM)
2. Quantity needed
3. Delivery location
4. Required delivery date

---

## 6. Pricing Response Guidelines

### If Price in CSV (non-zero):
```
Price: $XX.XX
Note: Pricing subject to confirmation. Contact EnPro for formal quote.
```

### If Price = $0 or Missing:
```
Price: Contact EnPro for current pricing
- Email: service@enproinc.com
- Phone: 1 (800) 323-2416

Typical range for [product type]: $XX-$XX [ESTIMATED]
```

### For Large Orders (100+ units):
```
Volume pricing available. Contact EnPro for quote.
```

---

## 7. Lead Time Guidelines

| Product Type | Typical Lead Time |
|--------------|-------------------|
| In-Stock Items | 1-3 days |
| Standard Cartridges | 1-2 weeks |
| Special Order | 2-4 weeks |
| Custom/Fabricated | 4-8 weeks |
| Import (Alteco, etc.) | 4-6 weeks |

---

**Version:** 25 (March 2026)
