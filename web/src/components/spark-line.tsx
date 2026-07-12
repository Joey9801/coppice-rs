import { Line, LineChart, ResponsiveContainer, YAxis } from 'recharts'

export interface SparkLineProps {
  data: Array<{ t: number; v: number }>
  color?: string
  height?: number
}

/** Tiny axis-less line chart for StatTile children. */
export function SparkLine({ data, color = 'var(--chart-1)', height = 36 }: SparkLineProps) {
  return (
    <ResponsiveContainer width="100%" height={height}>
      <LineChart data={data} margin={{ top: 2, right: 2, bottom: 2, left: 2 }}>
        <YAxis hide domain={['dataMin', 'dataMax']} />
        <Line
          type="monotone"
          dataKey="v"
          stroke={color}
          strokeWidth={1.5}
          dot={false}
          isAnimationActive={false}
        />
      </LineChart>
    </ResponsiveContainer>
  )
}
