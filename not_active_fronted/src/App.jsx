import { useState } from 'react'
import reactLogo from './assets/react.svg'
import viteLogo from '/vite.svg'
import './App.css'
import SQLEditor from './pages/SQLEditor.jsx'


function App() {
  const [count, setCount] = useState(0)

  return (
    <>
      <SQLEditor />
    </>
  )
}

export default App
