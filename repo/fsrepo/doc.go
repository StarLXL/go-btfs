// package fsrepo
//
// TODO explain the package roadmap...
//
//   .ipfs/
//   ├── client/
//   |   ├── client.lock          <------ protects client/ + signals its own pid
//   │   ├── ipfs-client.cpuprof
//   │   └── ipfs-client.memprof
//   ├── config
//   ├── daemon/
//   │   ├── daemon.lock          <------ protects daemon/ + signals its own address
//   │   ├── ipfs-daemon.cpuprof
//   │   └── ipfs-daemon.memprof
//   ├── ds/
//   ├── repo.lock                <------ protects ds/ and config
//   └── version
package fsrepo

// TODO prevent multiple daemons from running
