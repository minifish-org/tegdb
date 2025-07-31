use crate::Result;
use std::collections::{HashMap, HashSet};

/// HNSW (Hierarchical Navigable Small World) index for approximate nearest neighbor search
pub struct HNSWIndex {
    /// Maximum number of connections per layer
    max_connections: usize,
    /// Maximum number of connections for the top layer
    max_connections_top: usize,
    /// Number of layers in the hierarchy
    num_layers: usize,
    /// Current maximum layer
    max_layer: usize,
    /// Entry point (highest layer node)
    entry_point: Option<usize>,
    /// Nodes organized by layer: layer -> node_id -> neighbors
    layers: Vec<HashMap<usize, Vec<usize>>>,
    /// Vector data: node_id -> vector
    vectors: HashMap<usize, Vec<f64>>,
    /// Layer assignment for each node
    node_layers: HashMap<usize, usize>,
}

impl HNSWIndex {
    /// Create a new HNSW index
    pub fn new(max_connections: usize, max_connections_top: usize) -> Self {
        Self {
            max_connections,
            max_connections_top,
            num_layers: 16, // Default number of layers
            max_layer: 0,
            entry_point: None,
            layers: vec![HashMap::new(); 16],
            vectors: HashMap::new(),
            node_layers: HashMap::new(),
        }
    }

    /// Insert a vector into the index
    pub fn insert(&mut self, node_id: usize, vector: Vec<f64>) -> Result<()> {
        // Assign layer to the new node
        let layer = self.assign_layer();
        self.node_layers.insert(node_id, layer);
        self.vectors.insert(node_id, vector.clone());

        // Update max layer if needed
        if layer > self.max_layer {
            self.max_layer = layer;
        }

        // If this is the first node, set it as entry point
        if self.entry_point.is_none() {
            self.entry_point = Some(node_id);
            return Ok(());
        }

        // Find the entry point
        let entry_point = self.entry_point.unwrap();
        let entry_vector = self.vectors.get(&entry_point).unwrap();

        // Search for nearest neighbors starting from the top layer
        let mut current_ep = entry_point;
        let mut current_dist = cosine_distance(&vector, entry_vector);

        // Search from top layer down to layer + 1
        for layer_idx in (layer + 1..=self.max_layer).rev() {
            let layer_results = self.search_layer(&vector, current_ep, layer_idx, 1)?;
            if let Some((new_ep, new_dist)) = layer_results.first().copied() {
                if new_dist < current_dist {
                    current_ep = new_ep;
                    current_dist = new_dist;
                }
            }
        }

        // Search and connect at each layer from min(layer, max_layer) down to 0
        for layer_idx in (0..=layer.min(self.max_layer)).rev() {
            let layer_neighbors = self.search_layer(&vector, current_ep, layer_idx, self.max_connections)?;
            let neighbors = self.select_neighbors(&vector, &layer_neighbors, self.max_connections)?;
            
            // Connect the new node to its neighbors
            self.connect_node(node_id, &neighbors, layer_idx)?;
            
            // Connect neighbors to the new node
            for &neighbor_id in &neighbors {
                self.connect_node(neighbor_id, &[node_id], layer_idx)?;
            }

            current_ep = neighbors[0];
        }

        // Update entry point if the new node is at a higher layer
        if layer > *self.node_layers.get(&entry_point).unwrap_or(&0) {
            self.entry_point = Some(node_id);
        }

        Ok(())
    }

    /// Search for k nearest neighbors
    pub fn search(&self, query_vector: &[f64], k: usize) -> Result<Vec<(usize, f64)>> {
        if self.entry_point.is_none() {
            return Ok(Vec::new());
        }

        let entry_point = self.entry_point.unwrap();
        let entry_vector = self.vectors.get(&entry_point).unwrap();
        let mut current_dist = cosine_distance(query_vector, entry_vector);
        let mut current_ep = entry_point;

        // Search from top layer down to layer 0
        for layer_idx in (0..=self.max_layer).rev() {
            let layer_results = self.search_layer(query_vector, current_ep, layer_idx, 1)?;
            if let Some((new_ep, new_dist)) = layer_results.first().copied() {
                if new_dist < current_dist {
                    current_ep = new_ep;
                    current_dist = new_dist;
                }
            }
        }

        // Search at layer 0 with more candidates
        let candidates = self.search_layer(query_vector, current_ep, 0, k * 2)?;
        
        // Sort by distance and return top k
        let mut results: Vec<(usize, f64)> = candidates.into_iter().collect();
        results.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap());
        results.truncate(k);

        Ok(results)
    }

    /// Search within a specific layer
    fn search_layer(&self, query_vector: &[f64], entry_point: usize, layer: usize, ef: usize) -> Result<Vec<(usize, f64)>> {
        let mut candidates = HashSet::new();
        let mut visited = HashSet::new();
        let mut distances = HashMap::new();

        candidates.insert(entry_point);
        distances.insert(entry_point, cosine_distance(query_vector, self.vectors.get(&entry_point).unwrap()));

        while !candidates.is_empty() {
            // Find the closest candidate
            let current = *candidates.iter().min_by(|a, b| {
                distances.get(a).unwrap().partial_cmp(distances.get(b).unwrap()).unwrap()
            }).unwrap();

            candidates.remove(&current);
            visited.insert(current);

            // Check if we can improve
            if candidates.len() >= ef {
                let furthest_candidate = candidates.iter().max_by(|a, b| {
                    distances.get(a).unwrap().partial_cmp(distances.get(b).unwrap()).unwrap()
                }).unwrap();
                if distances.get(&current).unwrap() > distances.get(furthest_candidate).unwrap() {
                    break;
                }
            }

            // Explore neighbors
            if let Some(neighbors) = self.layers[layer].get(&current) {
                for &neighbor in neighbors {
                    if !visited.contains(&neighbor) {
                        let dist = cosine_distance(query_vector, self.vectors.get(&neighbor).unwrap());
                        candidates.insert(neighbor);
                        distances.insert(neighbor, dist);
                    }
                }
            }
        }

        let mut results: Vec<(usize, f64)> = visited.into_iter()
            .map(|id| (id, *distances.get(&id).unwrap()))
            .collect();
        results.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap());
        results.truncate(ef);

        Ok(results)
    }

    /// Select neighbors using the HNSW selection algorithm
    fn select_neighbors(&self, _query_vector: &[f64], candidates: &[(usize, f64)], m: usize) -> Result<Vec<usize>> {
        let mut selected = Vec::new();
        let mut candidates = candidates.to_vec();

        while selected.len() < m && !candidates.is_empty() {
            // Find the closest candidate
            let (closest_id, _) = candidates.remove(0);
            selected.push(closest_id);

            // Remove candidates that are closer to the selected candidate than to the query
            candidates.retain(|(id, dist_to_query)| {
                let dist_to_selected = cosine_distance(
                    self.vectors.get(id).unwrap(),
                    self.vectors.get(&closest_id).unwrap()
                );
                dist_to_selected > *dist_to_query
            });
        }

        Ok(selected)
    }

    /// Connect a node to its neighbors at a specific layer
    fn connect_node(&mut self, node_id: usize, neighbors: &[usize], layer: usize) -> Result<()> {
        let max_conn = if layer == 0 { self.max_connections } else { self.max_connections_top };
        
        let mut current_neighbors = self.layers[layer].get(&node_id).cloned().unwrap_or_default();
        current_neighbors.extend_from_slice(neighbors);
        
        // Limit connections
        if current_neighbors.len() > max_conn {
            // Simple truncation - in practice, you'd want more sophisticated selection
            current_neighbors.truncate(max_conn);
        }
        
        self.layers[layer].insert(node_id, current_neighbors);
        Ok(())
    }

    /// Assign a layer to a new node using the layer assignment algorithm
    fn assign_layer(&self) -> usize {
        let mut layer = 0;
        let mut rng = fastrand::Rng::new();
        
        while layer < self.num_layers - 1 && rng.f64() < 0.5 {
            layer += 1;
        }
        
        layer
    }

    /// Remove a vector from the index
    pub fn remove(&mut self, node_id: usize) -> Result<()> {
        if let Some(layer) = self.node_layers.remove(&node_id) {
            // Remove from layers
            for layer_idx in 0..=layer {
                if let Some(neighbors) = self.layers[layer_idx].remove(&node_id) {
                    // Remove this node from all its neighbors' neighbor lists
                    for neighbor_id in neighbors {
                        if let Some(neighbor_neighbors) = self.layers[layer_idx].get_mut(&neighbor_id) {
                            neighbor_neighbors.retain(|&id| id != node_id);
                        }
                    }
                }
            }
            
            // Remove vector data
            self.vectors.remove(&node_id);
            
            // Update entry point if needed
            if self.entry_point == Some(node_id) {
                self.entry_point = self.find_new_entry_point();
            }
        }
        
        Ok(())
    }

    /// Find a new entry point after removing the current one
    fn find_new_entry_point(&self) -> Option<usize> {
        // Find the node with the highest layer
        self.node_layers.iter()
            .max_by_key(|(_, &layer)| layer)
            .map(|(&id, _)| id)
    }

    /// Get the number of vectors in the index
    pub fn len(&self) -> usize {
        self.vectors.len()
    }

    /// Check if the index is empty
    pub fn is_empty(&self) -> bool {
        self.vectors.is_empty()
    }
}

/// Calculate cosine distance between two vectors
fn cosine_distance(a: &[f64], b: &[f64]) -> f64 {
    if a.len() != b.len() {
        return f64::INFINITY;
    }
    
    let mut dot_product = 0.0;
    let mut norm_a = 0.0;
    let mut norm_b = 0.0;
    
    for (x, y) in a.iter().zip(b.iter()) {
        dot_product += x * y;
        norm_a += x * x;
        norm_b += y * y;
    }
    
    if norm_a == 0.0 || norm_b == 0.0 {
        return 1.0; // Maximum distance for zero vectors
    }
    
    let cosine_similarity = dot_product / (norm_a.sqrt() * norm_b.sqrt());
    1.0 - cosine_similarity // Convert to distance
}

/// IVF (Inverted File Index) for clustering-based search
pub struct IVFIndex {
    /// Number of clusters
    num_clusters: usize,
    /// Cluster centroids
    centroids: Vec<Vec<f64>>,
    /// Cluster assignments: cluster_id -> vector_ids
    clusters: Vec<Vec<usize>>,
    /// Vector data: vector_id -> vector
    vectors: HashMap<usize, Vec<f64>>,
    /// Vector to cluster mapping
    vector_to_cluster: HashMap<usize, usize>,
}

impl IVFIndex {
    /// Create a new IVF index
    pub fn new(num_clusters: usize) -> Self {
        Self {
            num_clusters,
            centroids: Vec::new(),
            clusters: vec![Vec::new(); num_clusters],
            vectors: HashMap::new(),
            vector_to_cluster: HashMap::new(),
        }
    }

    /// Build the index from a set of vectors
    pub fn build(&mut self, vectors: Vec<(usize, Vec<f64>)>) -> Result<()> {
        if vectors.is_empty() {
            return Ok(());
        }

        // Store vectors first
        for (vector_id, vector) in &vectors {
            self.vectors.insert(*vector_id, vector.clone());
        }

        // Initialize centroids randomly
        self.initialize_centroids(&vectors)?;
        
        // K-means clustering
        for _ in 0..10 { // Max iterations
            self.assign_to_clusters(&vectors)?;
            self.update_centroids()?;
        }

        // Final assignment
        self.assign_to_clusters(&vectors)?;
        
        Ok(())
    }

    /// Initialize centroids randomly
    fn initialize_centroids(&mut self, vectors: &[(usize, Vec<f64>)]) -> Result<()> {
        self.centroids.clear();
        let _dimension = vectors[0].1.len();
        
        for _ in 0..self.num_clusters {
            let random_idx = fastrand::usize(..vectors.len());
            self.centroids.push(vectors[random_idx].1.clone());
        }
        
        Ok(())
    }

    /// Assign vectors to clusters
    fn assign_to_clusters(&mut self, vectors: &[(usize, Vec<f64>)]) -> Result<()> {
        // Clear clusters
        for cluster in &mut self.clusters {
            cluster.clear();
        }
        
        // Assign each vector to nearest centroid
        for (vector_id, vector) in vectors {
            let mut min_dist = f64::INFINITY;
            let mut best_cluster = 0;
            
            for (cluster_id, centroid) in self.centroids.iter().enumerate() {
                let dist = euclidean_distance(vector, centroid);
                if dist < min_dist {
                    min_dist = dist;
                    best_cluster = cluster_id;
                }
            }
            
            self.clusters[best_cluster].push(*vector_id);
            self.vector_to_cluster.insert(*vector_id, best_cluster);
        }
        
        Ok(())
    }

    /// Update centroids based on current cluster assignments
    fn update_centroids(&mut self) -> Result<()> {
        for (cluster_id, cluster) in self.clusters.iter().enumerate() {
            if cluster.is_empty() {
                continue;
            }
            
            let dimension = self.centroids[cluster_id].len();
            let mut new_centroid = vec![0.0; dimension];
            
            for &vector_id in cluster {
                if let Some(vector) = self.vectors.get(&vector_id) {
                    for (i, &val) in vector.iter().enumerate() {
                        new_centroid[i] += val;
                    }
                }
            }
            
            // Average
            let cluster_size = cluster.len() as f64;
            for val in &mut new_centroid {
                *val /= cluster_size;
            }
            
            self.centroids[cluster_id] = new_centroid;
        }
        
        Ok(())
    }

    /// Search for k nearest neighbors
    pub fn search(&self, query_vector: &[f64], k: usize) -> Result<Vec<(usize, f64)>> {
        // Find the closest centroid
        let mut min_dist = f64::INFINITY;
        let mut best_cluster = 0;
        
        for (cluster_id, centroid) in self.centroids.iter().enumerate() {
            let dist = euclidean_distance(query_vector, centroid);
            if dist < min_dist {
                min_dist = dist;
                best_cluster = cluster_id;
            }
        }
        
        // Search within the best cluster
        let mut results = Vec::new();
        for &vector_id in &self.clusters[best_cluster] {
            if let Some(vector) = self.vectors.get(&vector_id) {
                let dist = euclidean_distance(query_vector, vector);
                results.push((vector_id, dist));
            }
        }
        
        // Sort by distance and return top k
        results.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap());
        results.truncate(k);
        
        Ok(results)
    }

    /// Insert a vector into the index
    pub fn insert(&mut self, vector_id: usize, vector: Vec<f64>) -> Result<()> {
        self.vectors.insert(vector_id, vector.clone());
        
        // Find closest centroid
        let mut min_dist = f64::INFINITY;
        let mut best_cluster = 0;
        
        for (cluster_id, centroid) in self.centroids.iter().enumerate() {
            let dist = euclidean_distance(&vector, centroid);
            if dist < min_dist {
                min_dist = dist;
                best_cluster = cluster_id;
            }
        }
        
        // Assign to cluster
        self.clusters[best_cluster].push(vector_id);
        self.vector_to_cluster.insert(vector_id, best_cluster);
        
        Ok(())
    }
}

/// LSH (Locality Sensitive Hashing) for high-dimensional similarity search
pub struct LSHIndex {
    /// Number of hash tables
    num_tables: usize,
    /// Number of hash functions per table
    num_functions: usize,
    /// Hash tables: table_id -> hash_value -> vector_ids
    hash_tables: Vec<HashMap<u64, Vec<usize>>>,
    /// Vector data: vector_id -> vector
    vectors: HashMap<usize, Vec<f64>>,
    /// Random projections for hash functions
    projections: Vec<Vec<f64>>,
    /// Random offsets for hash functions
    offsets: Vec<f64>,
}

impl LSHIndex {
    /// Create a new LSH index
    pub fn new(num_tables: usize, num_functions: usize, dimension: usize) -> Self {
        let mut rng = fastrand::Rng::new();
        let mut projections = Vec::new();
        let mut offsets = Vec::new();
        
        // Generate random projections and offsets
        for _ in 0..num_tables * num_functions {
            let mut projection = Vec::new();
            for _ in 0..dimension {
                projection.push(rng.f64() * 2.0 - 1.0); // Random values in [-1, 1]
            }
            projections.push(projection);
            offsets.push(rng.f64() * 4.0); // Random offset
        }
        
        Self {
            num_tables,
            num_functions,
            hash_tables: vec![HashMap::new(); num_tables],
            vectors: HashMap::new(),
            projections,
            offsets,
        }
    }

    /// Insert a vector into the index
    pub fn insert(&mut self, vector_id: usize, vector: Vec<f64>) -> Result<()> {
        self.vectors.insert(vector_id, vector.clone());
        
        // Compute hash values for all tables
        for table_id in 0..self.num_tables {
            let hash_value = self.compute_hash(&vector, table_id);
            self.hash_tables[table_id].entry(hash_value).or_insert_with(Vec::new).push(vector_id);
        }
        
        Ok(())
    }

    /// Search for similar vectors
    pub fn search(&self, query_vector: &[f64], k: usize) -> Result<Vec<(usize, f64)>> {
        let mut candidates = HashSet::new();
        
        // Collect candidates from all hash tables
        for table_id in 0..self.num_tables {
            let hash_value = self.compute_hash(query_vector, table_id);
            if let Some(vector_ids) = self.hash_tables[table_id].get(&hash_value) {
                candidates.extend(vector_ids);
            }
        }
        
        // Compute actual distances for candidates
        let mut results = Vec::new();
        for &vector_id in &candidates {
            if let Some(vector) = self.vectors.get(&vector_id) {
                let dist = cosine_distance(query_vector, vector);
                results.push((vector_id, dist));
            }
        }
        
        // Sort by distance and return top k
        results.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap());
        results.truncate(k);
        
        Ok(results)
    }

    /// Compute hash value for a vector in a specific table
    fn compute_hash(&self, vector: &[f64], table_id: usize) -> u64 {
        let mut hash_value = 0u64;
        
        for func_id in 0..self.num_functions {
            let idx = table_id * self.num_functions + func_id;
            let projection = &self.projections[idx];
            let offset = self.offsets[idx];
            
            // Compute dot product with random projection
            let mut dot_product = 0.0;
            for (x, y) in vector.iter().zip(projection.iter()) {
                dot_product += x * y;
            }
            
            // Add offset and quantize
            let quantized = ((dot_product + offset) / 4.0) as u64;
            hash_value = hash_value.wrapping_mul(31).wrapping_add(quantized);
        }
        
        hash_value
    }

    /// Get the number of vectors in the index
    pub fn len(&self) -> usize {
        self.vectors.len()
    }

    /// Check if the index is empty
    pub fn is_empty(&self) -> bool {
        self.vectors.is_empty()
    }
}

/// Calculate Euclidean distance between two vectors
fn euclidean_distance(a: &[f64], b: &[f64]) -> f64 {
    if a.len() != b.len() {
        return f64::INFINITY;
    }
    
    let mut sum = 0.0;
    for (x, y) in a.iter().zip(b.iter()) {
        let diff = x - y;
        sum += diff * diff;
    }
    
    sum.sqrt()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_hnsw_basic() {
        let mut index = HNSWIndex::new(16, 32);
        
        // Insert some test vectors
        index.insert(1, vec![1.0, 0.0, 0.0]).unwrap();
        index.insert(2, vec![0.0, 1.0, 0.0]).unwrap();
        index.insert(3, vec![0.0, 0.0, 1.0]).unwrap();
        
        // Search
        let results = index.search(&[0.8, 0.2, 0.0], 2).unwrap();
        assert_eq!(results.len(), 2);
        assert_eq!(results[0].0, 1); // Should find vector 1 first
    }

    #[test]
    fn test_ivf_basic() {
        let mut index = IVFIndex::new(2);
        
        let vectors = vec![
            (1, vec![1.0, 0.0]),
            (2, vec![0.0, 1.0]),
            (3, vec![0.9, 0.1]),
            (4, vec![0.1, 0.9]),
        ];
        
        index.build(vectors).unwrap();
        
        // Search
        let results = index.search(&[0.8, 0.2], 2).unwrap();
        assert_eq!(results.len(), 2);
    }
} 