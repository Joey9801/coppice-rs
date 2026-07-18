//! Whole-physical-core allocation for Docker cpusets (§6.3).
//!
//! The pure allocator works in complete SMT sibling groups.  The small sysfs
//! reader at the bottom supplies those groups on Linux; tests feed synthetic
//! topologies so allocation correctness never depends on the test host.

use std::collections::{BTreeMap, BTreeSet};
use std::io;
use std::path::Path;

use coppice_core::id::AllocationId;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Core {
    pub(crate) cpus: BTreeSet<u32>,
    pub(crate) numa_node: u32,
}

#[derive(Debug, Clone)]
pub(crate) struct Topology {
    cores: Vec<Core>,
}

impl Topology {
    pub(crate) fn discover() -> io::Result<Self> {
        Self::from_sysfs(Path::new("/sys/devices/system/cpu"))
    }

    pub(crate) fn physical_cores(&self) -> usize {
        self.cores.len()
    }

    fn from_sysfs(root: &Path) -> io::Result<Self> {
        let mut unique = BTreeMap::<Vec<u32>, u32>::new();
        for entry in std::fs::read_dir(root)? {
            let entry = entry?;
            let name = entry.file_name();
            let name = name.to_string_lossy();
            let Some(cpu_id) = name.strip_prefix("cpu").and_then(|v| v.parse::<u32>().ok()) else {
                continue;
            };
            let siblings_path = entry.path().join("topology/thread_siblings_list");
            let raw = match std::fs::read_to_string(&siblings_path) {
                Ok(raw) => raw,
                Err(err) if err.kind() == io::ErrorKind::NotFound => continue,
                Err(err) => return Err(err),
            };
            let siblings = parse_cpu_list(raw.trim()).map_err(|message| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("{}: {message}", siblings_path.display()),
                )
            })?;
            if !siblings.contains(&cpu_id) {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!(
                        "{} does not contain its cpu id {cpu_id}",
                        siblings_path.display()
                    ),
                ));
            }
            let mut numa = 0;
            for child in std::fs::read_dir(entry.path())? {
                let child = child?;
                let child_name = child.file_name();
                let child_name = child_name.to_string_lossy();
                if let Some(node) = child_name
                    .strip_prefix("node")
                    .and_then(|v| v.parse::<u32>().ok())
                {
                    numa = node;
                    break;
                }
            }
            unique.entry(siblings).or_insert(numa);
        }
        if unique.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::NotFound,
                format!("no CPU topology found under {}", root.display()),
            ));
        }
        let mut cores: Vec<_> = unique
            .into_iter()
            .map(|(cpus, numa_node)| Core {
                cpus: cpus.into_iter().collect(),
                numa_node,
            })
            .collect();
        cores.sort_by_key(|core| (core.numa_node, core.cpus.iter().next().copied()));
        Ok(Self { cores })
    }

    #[cfg(test)]
    fn synthetic(groups: &[(u32, &[u32])]) -> Self {
        Self {
            cores: groups
                .iter()
                .map(|(numa_node, cpus)| Core {
                    cpus: cpus.iter().copied().collect(),
                    numa_node: *numa_node,
                })
                .collect(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Affinity {
    pub(crate) cpuset_cpus: String,
    pub(crate) nano_cpus: i64,
    pub(crate) exclusive: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Allocation {
    pub(crate) affinity: Affinity,
    pub(crate) newly_assigned: bool,
}

#[derive(Debug)]
pub(crate) struct Allocator {
    cores: Vec<Core>,
    reserved: BTreeSet<usize>,
    grants: BTreeMap<AllocationId, Vec<usize>>,
    fractional: BTreeSet<AllocationId>,
}

impl Allocator {
    pub(crate) fn new(topology: Topology, reservation_cpu_millis: u64) -> Result<Self, String> {
        let reserved_count = usize::try_from(reservation_cpu_millis / 1000)
            .map_err(|_| "reservation core count does not fit usize".to_string())?;
        if reserved_count > topology.cores.len() {
            return Err(format!(
                "reservation needs {reserved_count} physical cores but topology has {}",
                topology.cores.len()
            ));
        }
        Ok(Self {
            cores: topology.cores,
            reserved: (0..reserved_count).collect(),
            grants: BTreeMap::new(),
            fractional: BTreeSet::new(),
        })
    }

    pub(crate) fn allocate(
        &mut self,
        allocation: AllocationId,
        cpu_millis: u64,
    ) -> Result<Allocation, String> {
        if let Some(indices) = self.grants.get(&allocation) {
            return Ok(Allocation {
                affinity: self.exclusive_affinity(indices),
                newly_assigned: false,
            });
        }
        if self.fractional.contains(&allocation) {
            return Ok(Allocation {
                affinity: self.fractional_affinity(cpu_millis),
                newly_assigned: false,
            });
        }

        if cpu_millis > 0 && cpu_millis % 1000 == 0 {
            let count = usize::try_from(cpu_millis / 1000)
                .map_err(|_| "whole-core request does not fit usize".to_string())?;
            let indices = self.choose_cores(count).ok_or_else(|| {
                format!(
                    "cpuset invariant breach: requested {count} physical cores, only {} free",
                    self.free_indices().len()
                )
            })?;
            let affinity = self.exclusive_affinity(&indices);
            self.grants.insert(allocation, indices);
            Ok(Allocation {
                affinity,
                newly_assigned: true,
            })
        } else {
            self.fractional.insert(allocation);
            Ok(Allocation {
                affinity: self.fractional_affinity(cpu_millis),
                newly_assigned: true,
            })
        }
    }

    pub(crate) fn rebuild_exclusive(
        &mut self,
        allocation: AllocationId,
        cpuset: &str,
    ) -> Result<(), String> {
        let cpus: BTreeSet<_> = parse_cpu_list(cpuset)?.into_iter().collect();
        let mut indices = Vec::new();
        let mut rebuilt = BTreeSet::new();
        for (index, core) in self.cores.iter().enumerate() {
            if core.cpus.is_subset(&cpus) {
                indices.push(index);
                rebuilt.extend(core.cpus.iter().copied());
            }
        }
        if rebuilt != cpus || indices.is_empty() {
            return Err(format!(
                "surviving grant {allocation} cpuset {cpuset:?} is not a union of whole sibling groups"
            ));
        }
        if indices.iter().any(|i| self.reserved.contains(i)) {
            return Err(format!(
                "surviving grant {allocation} overlaps the system reservation"
            ));
        }
        let occupied: BTreeSet<_> = self.grants.values().flatten().copied().collect();
        if indices.iter().any(|i| occupied.contains(i)) {
            return Err(format!(
                "surviving grant {allocation} overlaps another exclusive grant"
            ));
        }
        self.grants.insert(allocation, indices);
        Ok(())
    }

    pub(crate) fn rebuild_fractional(&mut self, allocation: AllocationId) {
        self.fractional.insert(allocation);
    }

    pub(crate) fn release(&mut self, allocation: AllocationId) -> bool {
        self.grants.remove(&allocation).is_some() | self.fractional.remove(&allocation)
    }

    pub(crate) fn fractional_allocations(&self) -> Vec<AllocationId> {
        self.fractional.iter().copied().collect()
    }

    pub(crate) fn shared_cpuset(&self) -> String {
        format_cpu_set(self.shared_cpus())
    }

    fn choose_cores(&self, count: usize) -> Option<Vec<usize>> {
        let free = self.free_indices();
        if free.len() < count {
            return None;
        }
        let mut by_node = BTreeMap::<u32, Vec<usize>>::new();
        for &index in &free {
            by_node
                .entry(self.cores[index].numa_node)
                .or_default()
                .push(index);
        }
        if let Some(indices) = by_node.values().find(|indices| indices.len() >= count) {
            return Some(indices[..count].to_vec());
        }
        Some(free.into_iter().take(count).collect())
    }

    fn free_indices(&self) -> Vec<usize> {
        let granted: BTreeSet<_> = self.grants.values().flatten().copied().collect();
        (0..self.cores.len())
            .filter(|i| !self.reserved.contains(i) && !granted.contains(i))
            .collect()
    }

    fn shared_cpus(&self) -> BTreeSet<u32> {
        self.free_indices()
            .into_iter()
            .flat_map(|i| self.cores[i].cpus.iter().copied())
            .collect()
    }

    fn fractional_affinity(&self, cpu_millis: u64) -> Affinity {
        Affinity {
            cpuset_cpus: self.shared_cpuset(),
            nano_cpus: millis_to_nanos(cpu_millis),
            exclusive: false,
        }
    }

    fn exclusive_affinity(&self, indices: &[usize]) -> Affinity {
        let cpus: BTreeSet<_> = indices
            .iter()
            .flat_map(|&i| self.cores[i].cpus.iter().copied())
            .collect();
        Affinity {
            cpuset_cpus: format_cpu_set(cpus.iter().copied()),
            nano_cpus: i64::try_from(cpus.len())
                .unwrap_or(i64::MAX)
                .saturating_mul(1_000_000_000),
            exclusive: true,
        }
    }
}

pub(crate) fn millis_to_nanos(cpu_millis: u64) -> i64 {
    i64::try_from(cpu_millis)
        .unwrap_or(i64::MAX)
        .saturating_mul(1_000_000)
}

fn parse_cpu_list(raw: &str) -> Result<Vec<u32>, String> {
    let mut cpus = BTreeSet::new();
    if raw.is_empty() {
        return Ok(Vec::new());
    }
    for part in raw.split(',') {
        if let Some((start, end)) = part.split_once('-') {
            let start: u32 = start
                .parse()
                .map_err(|_| format!("invalid CPU range {part:?}"))?;
            let end: u32 = end
                .parse()
                .map_err(|_| format!("invalid CPU range {part:?}"))?;
            if start > end {
                return Err(format!("descending CPU range {part:?}"));
            }
            cpus.extend(start..=end);
        } else {
            cpus.insert(
                part.parse()
                    .map_err(|_| format!("invalid CPU id {part:?}"))?,
            );
        }
    }
    Ok(cpus.into_iter().collect())
}

fn format_cpu_set(cpus: impl IntoIterator<Item = u32>) -> String {
    cpus.into_iter()
        .map(|cpu| cpu.to_string())
        .collect::<Vec<_>>()
        .join(",")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn alloc() -> AllocationId {
        AllocationId::new()
    }

    #[test]
    fn smt_grants_are_complete_and_pool_is_the_complement() {
        let topology = Topology::synthetic(&[(0, &[0, 4]), (0, &[1, 5]), (1, &[2, 6])]);
        let mut allocator = Allocator::new(topology, 0).unwrap();
        let grant = allocator.allocate(alloc(), 1000).unwrap().affinity;
        assert_eq!(grant.cpuset_cpus, "0,4");
        assert_eq!(grant.nano_cpus, 2_000_000_000);
        assert_eq!(allocator.shared_cpuset(), "1,2,5,6");
    }

    #[test]
    fn non_smt_grants_still_take_whole_groups() {
        let topology = Topology::synthetic(&[(0, &[0]), (0, &[1]), (0, &[2])]);
        let mut allocator = Allocator::new(topology, 0).unwrap();
        let grant = allocator.allocate(alloc(), 2000).unwrap().affinity;
        assert_eq!(grant.cpuset_cpus, "0,1");
        assert_eq!(grant.nano_cpus, 2_000_000_000);
    }

    #[test]
    fn numa_packs_when_possible_and_spills_when_required() {
        let topology =
            Topology::synthetic(&[(0, &[0]), (0, &[1]), (1, &[2]), (1, &[3]), (1, &[4])]);
        let mut allocator = Allocator::new(topology, 0).unwrap();
        assert_eq!(
            allocator
                .allocate(alloc(), 3000)
                .unwrap()
                .affinity
                .cpuset_cpus,
            "2,3,4"
        );
        assert_eq!(
            allocator
                .allocate(alloc(), 2000)
                .unwrap()
                .affinity
                .cpuset_cpus,
            "0,1"
        );

        let topology = Topology::synthetic(&[(0, &[0]), (1, &[1]), (1, &[2])]);
        let mut allocator = Allocator::new(topology, 0).unwrap();
        assert_eq!(
            allocator
                .allocate(alloc(), 3000)
                .unwrap()
                .affinity
                .cpuset_cpus,
            "0,1,2"
        );
    }

    #[test]
    fn reservation_carves_out_whole_sibling_groups() {
        let topology = Topology::synthetic(&[(0, &[0, 4]), (0, &[1, 5]), (0, &[2, 6])]);
        let mut allocator = Allocator::new(topology, 1500).unwrap();
        assert_eq!(allocator.shared_cpuset(), "1,2,5,6");
        assert_eq!(
            allocator
                .allocate(alloc(), 1000)
                .unwrap()
                .affinity
                .cpuset_cpus,
            "1,5"
        );
    }

    #[test]
    fn fractional_pool_shrinks_and_grows_with_grants() {
        let topology = Topology::synthetic(&[(0, &[0, 4]), (0, &[1, 5]), (0, &[2, 6])]);
        let mut allocator = Allocator::new(topology, 0).unwrap();
        let fractional = alloc();
        let whole = alloc();
        assert_eq!(
            allocator
                .allocate(fractional, 500)
                .unwrap()
                .affinity
                .cpuset_cpus,
            "0,1,2,4,5,6"
        );
        allocator.allocate(whole, 1000).unwrap();
        assert_eq!(allocator.shared_cpuset(), "1,2,5,6");
        allocator.release(whole);
        assert_eq!(allocator.shared_cpuset(), "0,1,2,4,5,6");
    }

    #[test]
    fn invariant_breach_refuses_instead_of_falling_back() {
        let topology = Topology::synthetic(&[(0, &[0]), (0, &[1])]);
        let mut allocator = Allocator::new(topology, 1000).unwrap();
        let err = allocator.allocate(alloc(), 2000).unwrap_err();
        assert!(err.contains("invariant breach"));
        assert!(allocator.grants.is_empty());
    }

    #[test]
    fn rebuild_accepts_only_complete_non_overlapping_groups() {
        let topology = Topology::synthetic(&[(0, &[0, 4]), (0, &[1, 5]), (0, &[2, 6])]);
        let mut allocator = Allocator::new(topology, 0).unwrap();
        allocator.rebuild_exclusive(alloc(), "1,5").unwrap();
        assert_eq!(allocator.shared_cpuset(), "0,2,4,6");
        assert!(allocator.rebuild_exclusive(alloc(), "0").is_err());
        assert!(allocator.rebuild_exclusive(alloc(), "1,5").is_err());
    }

    #[test]
    fn cpu_list_parses_kernel_range_syntax() {
        assert_eq!(
            parse_cpu_list("0-2,8,10-11").unwrap(),
            vec![0, 1, 2, 8, 10, 11]
        );
    }
}
