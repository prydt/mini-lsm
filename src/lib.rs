use std::{
    collections::{hash_map::DefaultHasher, BTreeMap},
    fs::{self, File, OpenOptions},
    hash::{Hash, Hasher},
    io::{BufReader, Write},
    path::Path,
};

use anyhow::Result;
use bincode::{Decode, Encode};
struct Lsm<'a, K, V> {
    // memtable
    memtable: BTreeMap<K, V>,

    // TODO change to size in bytes??
    // max size of memtable before flush
    // use std::mem::size_of_val
    max_size: usize,
    // log
    wal: File,
    // manifest handle
    // manifest: File,
    manifest_path: &'a Path,
    // current SSTs
    tables: Vec<String>,
}

#[derive(Encode, Decode, Debug)]
struct LogEntry<
    K: Encode + Decode + Hash + Ord + 'static,
    V: Encode + Decode + Hash + Ord + 'static,
> {
    crc: u32,
    is_tombstone: bool,
    key: K,
    value: V,
}

#[derive(Encode, Decode, Debug)]
struct Sst<K: 'static, V: 'static> {
    entries: Vec<(K, V)>,
}

impl<'a, K, V> Lsm<'a, K, V>
where
    K: Encode + Decode + Hash + Ord + 'static,
    V: Encode + Decode + Hash + Ord + Clone + 'static,
{
    /// Makes a new LSM Handle
    ///
    fn new(path: &Path) -> Lsm<K, V> {
        // check if manifest exists
        // read manifest, set tables
        // else
        // make manifest

        let manifest_content = if path.is_file() {
            let content = fs::read(path).unwrap(); // TODO add error checking
            bincode::decode_from_slice::<Vec<String>, _>(&content, bincode::config::standard())
                .unwrap()
                .0
        } else {
            vec![]
        };

        // make/recover log
        let memtable = Self::try_log_recovery(Path::new(".log")).unwrap_or(BTreeMap::new());

        Lsm {
            memtable,
            max_size: 2, // TODO make this a parameter
            // wal: File::open(Path::new(".log")).unwrap(),
            wal: OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .append(true)
                .open(".log")
                .unwrap(),
            // manifest: manifest_file,
            manifest_path: path,
            tables: manifest_content,
        }
    }

    ///
    /// Puts a key-value pair into the LSM tree.
    ///
    ///
    fn put(&mut self, key: K, value: V) -> Result<usize> {
        if self.memtable.len() >= self.max_size {
            // dump memtable to sst

            let dump: Vec<(K, V)> = std::mem::take(&mut self.memtable).into_iter().collect();
            let payload = bincode::encode_to_vec(dump, bincode::config::standard())?;

            let name = format!(
                "sst{:03}{}",
                self.tables.len(),
                self.manifest_path.file_stem().unwrap().to_str().unwrap()
            );
            let mut table = fs::OpenOptions::new()
                .write(true)
                .create(true)
                .open(Path::new(&name))?;
            table.write_all(&payload)?;
            table.flush()?;
            self.tables.push(name);
            self.write_manifest()?;

            self.wal = OpenOptions::new()
                .write(true)
                .create(true)
                .truncate(true)
                .open(".log")?;
        }

        // bincode::encode_into_writer( Self::new_wal_entry(false, key, value), self.wal, bincode::config::standard());
        let entry = Self::new_wal_entry(false, key, value);
        let payload = bincode::encode_to_vec(&entry, bincode::config::standard())?;

        let bytes_written = self.wal.write(&payload)?;
        self.wal.flush()?;

        self.memtable.insert(entry.key, entry.value);

        Ok(bytes_written)
    }

    ///
    /// Gets a value addressed by key from the LSM Tree.
    ///
    /// Returns None if not present.
    ///
    fn get(&self, key: &K) -> Option<V> {
        if let Some(value) = self.memtable.get(key) {
            return Some(value.clone());
        }

        // search through all tables
        for table in self.tables.iter().rev() {
            let mut reader = BufReader::new(File::open(Path::new(table)).unwrap()); // TODO error checking

            let sst = bincode::decode_from_reader::<Sst<K, V>, &mut BufReader<File>, _>(
                &mut reader,
                bincode::config::standard(),
            )
            .unwrap();

            let search = sst.entries.binary_search_by_key(&key, |(k, _)| k);
            if let Ok(index) = search {
                return Some(sst.entries.get(index).unwrap().1.clone());
            }
        }

        None
    }

    fn write_manifest(&mut self) -> Result<()> {
        let mut manifest = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(self.manifest_path)
            .unwrap();
        manifest.write_all(&bincode::encode_to_vec(
            &self.tables,
            bincode::config::standard(),
        )?)?;
        manifest.flush()?;

        Ok(())
    }

    fn try_log_recovery(log_path: &Path) -> Result<BTreeMap<K, V>, ()> {
        if log_path.is_file() {
            let mut memtable: BTreeMap<K, V> = BTreeMap::new();

            let mut input_reader = BufReader::new(File::open(log_path).unwrap());
            while let Ok(entry) = bincode::decode_from_reader::<
                LogEntry<K, V>,
                &mut BufReader<File>,
                _,
            >(&mut input_reader, bincode::config::standard())
            {
                if entry.crc == Self::compute_crc(&entry) {
                    if entry.is_tombstone {
                        memtable.remove(&entry.key);
                    } else {
                        memtable.insert(entry.key, entry.value);
                    }
                }
            }

            return Ok(memtable);
        }
        Err(())
    }

    fn new_wal_entry(is_tombstone: bool, key: K, value: V) -> LogEntry<K, V> {
        let mut entry = LogEntry {
            crc: 0,
            is_tombstone,
            key,
            value,
        };
        entry.crc = Self::compute_crc(&entry);
        entry
    }

    fn compute_hash<H: Hash>(elem: &H) -> u64 {
        let mut state = DefaultHasher::new();
        elem.hash(&mut state);
        state.finish()
    }

    fn compute_crc(entry: &LogEntry<K, V>) -> u32 {
        let mut hasher = crc32fast::Hasher::new();
        hasher.update(if entry.is_tombstone { &[1] } else { &[0] });
        hasher.update(&Self::compute_hash(&entry.key).to_le_bytes());
        hasher.update(&Self::compute_hash(&entry.value).to_le_bytes());
        hasher.finalize()
    }
}

// TODO write some actual tests for this

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_works() -> anyhow::Result<()> {
        let mut lsm: Lsm<String, u32> = Lsm::new(Path::new("test.lsm"));

        lsm.put("p".to_string(), 4)?;
        lsm.put("j".to_string(), 7)?;

        // println!("pranoy = {:?}", lsm.get(&"pranoy".to_string()));
        assert_eq!(lsm.get(&"p".to_string()), Some(4));

        lsm.put("b".to_string(), 10)?;

        // println!("pranoy = {}", lsm.get(&"pranoy".to_string()).unwrap());
        assert_eq!(lsm.get(&"p".to_string()), Some(4));

        lsm.put("a".to_string(), 2)?;
        // println!("june = {}", lsm.get(&"june".to_string()).unwrap());
        assert_eq!(lsm.get(&"j".to_string()), Some(7));

        lsm.put("t".to_string(), 3847)?;
        // println!("-----");
        // println!("pranoy = {}", lsm.get(&"pranoy".to_string()).unwrap());
        // println!("june = {}", lsm.get(&"june".to_string()).unwrap());
        // println!("bentry = {}", lsm.get(&"bentry".to_string()).unwrap());
        // println!("andy = {}", lsm.get(&"andy".to_string()).unwrap());
        // println!("tony = {}", lsm.get(&"tony".to_string()).unwrap());
        assert_eq!(lsm.get(&"p".to_string()), Some(4));
        assert_eq!(lsm.get(&"j".to_string()), Some(7));
        assert_eq!(lsm.get(&"b".to_string()), Some(10));
        assert_eq!(lsm.get(&"a".to_string()), Some(2));
        assert_eq!(lsm.get(&"t".to_string()), Some(3847));


        assert_eq!(4, 4);

        Ok(())
    }
}
