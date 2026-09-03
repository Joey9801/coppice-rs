import type { HostFacts } from '@/api/types'

// Keep this display fallback aligned with the canonical Rust labels in crates/coppice-core/src/node.rs.
const OS_LABELS: Record<string, string> = {
  aix: 'AIX',
  android: 'Android',
  darwin: 'macOS',
  dragonfly: 'DragonFly BSD',
  freebsd: 'FreeBSD',
  illumos: 'illumos',
  ios: 'iOS',
  linux: 'Linux',
  macos: 'macOS',
  netbsd: 'NetBSD',
  openbsd: 'OpenBSD',
  solaris: 'Solaris',
}

/** Display a host OS value, including a family label for older bare readings. */
export function formatHostOs(host: Pick<HostFacts, 'os' | 'osVersion'>): string | null {
  const value = host.osVersion.trim()
  if (!value) return osLabel(host.os) ?? (host.os.trim() || null)

  // Linux PRETTY_NAME values are already the useful human-readable name
  // (for example, "Debian GNU/Linux 12 (bookworm)"); do not prefix them.
  if (host.os.trim().toLowerCase() === 'linux') return value

  const label = osLabel(host.os)
  return label ? addLabel(value, label) : value
}

/** Display a kernel release with the family name needed to identify it. */
export function formatHostKernel(host: Pick<HostFacts, 'os' | 'kernelVersion'>): string | null {
  const value = host.kernelVersion.trim()
  if (!value) return null

  const label = kernelLabel(host.os)
  return label ? addLabel(value, label) : value
}

function osLabel(os: string): string | null {
  return OS_LABELS[os.trim().toLowerCase()] ?? null
}

function kernelLabel(os: string): string | null {
  switch (os.trim().toLowerCase()) {
    case 'android':
      return 'Linux'
    case 'ios':
    case 'macos':
    case 'darwin':
      return 'Darwin'
    case 'solaris':
      return 'SunOS'
    default:
      return osLabel(os)
  }
}

function addLabel(value: string, label: string): string {
  return hasLabel(value, label) ? value : `${label} ${value}`
}

function hasLabel(value: string, label: string): boolean {
  if (
    value.length < label.length ||
    value.slice(0, label.length).toLowerCase() !== label.toLowerCase()
  ) {
    return false
  }
  return value.length === label.length || /\s/.test(value[label.length] ?? '')
}
