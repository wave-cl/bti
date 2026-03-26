/// Stable Bloom Filter with automatic eviction of old entries.
/// Uses counter-based cells that decay over time.
pub struct StableBloomFilter {
    cells: Vec<u8>,
    num_cells: usize,
    num_hashes: usize,
    decay_count: usize,
    max_val: u8,
    insert_count: u64,
}

impl StableBloomFilter {
    /// Create a new stable bloom filter.
    /// - `capacity`: approximate number of items to track
    /// - `fp_rate`: desired false positive rate (e.g., 0.001)
    pub fn new(capacity: usize, fp_rate: f64) -> Self {
        let num_hashes = 3;
        // ~2 bytes per item gives acceptable FPR with counter-based decay
        let num_cells = (capacity * 2).max(1024);
        let decay_count = num_hashes; // decay same number of cells per insert

        Self {
            cells: vec![0; num_cells],
            num_cells,
            num_hashes,
            decay_count,
            max_val: 3, // small counter max
            insert_count: 0,
        }
    }

    /// Test if the item might be in the filter, and add it.
    /// Returns true if the item was likely already present.
    pub fn test_and_add(&mut self, data: &[u8; 20]) -> bool {
        // Decay random cells
        self.decay(data);

        // Test
        let mut present = true;
        let indices = self.hash_indices(data);
        for &idx in &indices {
            if self.cells[idx] == 0 {
                present = false;
            }
        }

        // Add
        for &idx in &indices {
            if self.cells[idx] < self.max_val {
                self.cells[idx] = self.max_val;
            }
        }

        self.insert_count += 1;
        present
    }

    /// Test without adding.
    pub fn test(&self, data: &[u8; 20]) -> bool {
        let indices = self.hash_indices(data);
        indices.iter().all(|&idx| self.cells[idx] > 0)
    }

    fn decay(&mut self, seed: &[u8; 20]) {
        // Use the seed to deterministically select cells to decay
        let base = u64::from_le_bytes([
            seed[0], seed[1], seed[2], seed[3], seed[4], seed[5], seed[6], seed[7],
        ]);
        for i in 0..self.decay_count {
            let idx = ((base.wrapping_add(i as u64).wrapping_mul(0x517cc1b727220a95)) % self.num_cells as u64) as usize;
            if self.cells[idx] > 0 {
                self.cells[idx] -= 1;
            }
        }
    }

    fn hash_indices(&self, data: &[u8; 20]) -> Vec<usize> {
        let h1 = u64::from_le_bytes([
            data[0], data[1], data[2], data[3], data[4], data[5], data[6], data[7],
        ]);
        let h2 = u64::from_le_bytes([
            data[8], data[9], data[10], data[11], data[12], data[13], data[14], data[15],
        ]);

        (0..self.num_hashes)
            .map(|i| {
                let h = h1.wrapping_add((i as u64).wrapping_mul(h2));
                (h % self.num_cells as u64) as usize
            })
            .collect()
    }
}
