interface Props {
  value: string;
  label?: string;
}

export default function Fingerprint({ value, label = "Match code" }: Props) {
  const short =
    value.length > 20 ? `${value.slice(0, 12)}…${value.slice(-6)}` : value;
  return (
    <div className="fingerprint" title={value}>
      <span className="fingerprint-label">{label}</span>
      <code className="fingerprint-code">{short}</code>
    </div>
  );
}
