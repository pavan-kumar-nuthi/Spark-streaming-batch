import { useState } from 'react'
import {
	CartesianGrid,
	Legend,
	Line,
	LineChart,
	ResponsiveContainer,
	Tooltip,
	XAxis,
	YAxis,
} from 'recharts'


function App() {
	const [data, setData] = useState<
		{
			best_ask: number
			best_bid: number
			price: number
		}[]
	>([])
	const [selVal, setSelVal] = useState('BTC-USD')

    const wsc = new WebSocket('ws://localhost:1337')
    wsc.onmessage = (message) => {
        console.log(JSON.parse(message.data));
    }


	return (
		<>
			<div className='h-[100vh] grid place-items-center'>
				<div className='grid grid-cols-3 gap-3 w-[1000px] h-[550px] p-4 rounded-lg bg-blue-900'>
					<div className='col-span-1'>
						<select
							className='block w-full mt-1 bg-blue-600 text-white rounded-md border-none shadow-sm focus:border-indigo-300 focus:ring focus:ring-indigo-200 focus:ring-opacity-50
                  '
							value={selVal}
							onChange={(e) => {
								setSelVal(e.target.value)
								setData([])
							}}
						>
							<option value='BTC-USD'>Bitcoin</option>
							<option value='ETH-USD'>Ethereum</option>
						</select>
					</div>
					<div className='col-span-2 bg-blue-600 rounded-md p-2 pr-10'>
						<ResponsiveContainer>
							<LineChart
								data={data.map((datum, index) => {
									return {
										...datum,
										index,
									}
								})}
							>
								<Legend verticalAlign='top' />
								<CartesianGrid stroke='#fff' strokeDasharray='5 5' />
								<Line type='monotone' dataKey='best_ask' stroke='#00f0ff' />
								<Line type='monotone' dataKey='best_bid' stroke='#ff0000' />
								<Line type='monotone' dataKey='price' stroke='#8884d8' />
								<XAxis dataKey='index' stroke='#fff' />
								<YAxis stroke='#fff' />
								<Tooltip />
							</LineChart>
						</ResponsiveContainer>
					</div>
				</div>
			</div>
		</>
	)
}

export default App
