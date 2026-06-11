export const Source = {
  DhtGetPeers: 1,
  DhtAnnouncePeer: 2,
  ManualMagnet: 3,
};

export function nowTs() {
  return Math.floor(Date.now() / 1000);
}
