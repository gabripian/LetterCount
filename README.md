# Letter Frequency Analysis System

This project implements a system that counts the frequency of the 26 letters of the English alphabet in `.txt` files, using both **MapReduce**-based and non-distributed approaches.  
The goal is to evaluate different strategies in terms of **speed**, **scalability**, and **efficiency**, to identify the most effective method for large-scale text processing.

---

## Overview

The basic algorithm:
1. Read the content of the input text file.  
2. Initialize an associative array with one entry for each letter (A–Z).  
3. For each character in the text, increment the counter of the corresponding letter.

Two distributed implementations have been developed:
- **Combiner approach** – Uses a separate combiner to pre-aggregate intermediate data before the reducer stage.  
- **In-Mapper Combining approach** – Each mapper maintains its own state, aggregating results locally before emitting key-value pairs.

---

## Testing and Dataset

The system was tested on several files of increasing size, derived from the *King James Bible (1611)*:
- `pg.txt` – 2 KB  
- `pg2.txt` – 2 MB  
- `pg22.txt` – 22 MB  
- `pg200.txt` – 200 MB  
- `pg1000.txt` – 1 GB  
- `pg5000.txt` – 5 GB  

Performance metrics include **execution time**, **mapper/reducer time**, and **bytes written**, analyzed under varying numbers of reducers.

---

## Not Distributed Implementations

For comparison, two additional standalone implementations were created:
- **Python version** – High-level interpreted approach.  
- **C++ version** – Low-level compiled version for maximum performance.  

These versions do not use MapReduce but process files sequentially or using multiple threads.

---

## Italian Letter Frequency Analysis

The project also includes an analysis of letter frequencies in eight Italian texts, from **19 B.C. to 2023 A.D.**, to study linguistic evolution:
- *Eneide* (Latin, 19 B.C.)  
- *Divina Commedia* (1321)  
- *Il Principe* (1532)  
- *I Promessi Sposi* (1827)  
- *Il Piacere* (1889)  
- *Costituzione Italiana* (1948)  
- *Bibbia di Gerusalemme* (1973)  
- *Statuto FIAT* (2023)

---

## Results and Conclusions

- **In-Mapper Combining** significantly reduces execution time compared to the combiner strategy.  
- Increasing reducers (from 1 to 3) yields limited improvements.  
- Compared to the C++ version, In-Mapper Combining scales better and handles large datasets more efficiently.

Overall, the **In-Mapper Combining** approach proves to be the most scalable and efficient method for distributed letter frequency analysis.
