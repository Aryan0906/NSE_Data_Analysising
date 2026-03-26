import plotly.graph_objects as go
import pandas as pd

def plot_candlestick(df: pd.DataFrame, ticker: str) -> go.Figure:
    """Create a beautiful plotly candlestick chart with SMAs."""
    fig = go.Figure()

    # Candlestick
    fig.add_trace(go.Candlestick(
        x=df['trading_date'],
        open=df['open_price'],
        high=df['high_price'],
        low=df['low_price'],
        close=df['close_price'],
        name='Price'
    ))

    # Add 20-day SMA if present
    if 'sma_20' in df.columns:
        fig.add_trace(go.Scatter(
            x=df['trading_date'], 
            y=df['sma_20'], 
            line=dict(color='orange', width=1.5), 
            name='20-Day SMA'
        ))

    # Add 50-day SMA if present
    if 'sma_50' in df.columns:
        fig.add_trace(go.Scatter(
            x=df['trading_date'], 
            y=df['sma_50'], 
            line=dict(color='blue', width=1.5), 
            name='50-Day SMA'
        ))

    fig.update_layout(
        title=f"{ticker} - Daily Price Action & Moving Averages",
        yaxis_title='Price (INR)',
        xaxis_title='Date',
        template='plotly_white',
        xaxis_rangeslider_visible=False,
        height=600,
        margin=dict(l=20, r=20, t=60, b=20)
    )
    
    return fig

def plot_volume_bar(df: pd.DataFrame) -> go.Figure:
    """Create a volume bar chart."""
    fig = go.Figure()
    
    # Color volume red if close < open, else green
    colors = ['red' if row['close_price'] < row['open_price'] else 'green' 
              for _, row in df.iterrows()]
              
    fig.add_trace(go.Bar(
        x=df['trading_date'],
        y=df['volume'],
        marker_color=colors,
        name='Volume'
    ))
    
    fig.update_layout(
        title="Trading Volume",
        yaxis_title="Volume",
        template='plotly_white',
        height=250,
        margin=dict(l=20, r=20, t=40, b=20)
    )
    
    return fig
